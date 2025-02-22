-- Copyright 2021 Kafka-Tarantool-Loader
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--     http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

---
--- Created by ashitov.
--- DateTime: 6/2/20 1:51 PM
---

local avro = require('avro')
local checks = require('checks')
local json = require('json')
local error_repository = require('app.messages.error_repository')
local misc_utils = require('app.utils.misc_utils')
local file_utils = require('app.utils.file_utils')
local fio = require('fio')

local sync_size = 16
local magic = string.char(0x4F, 0x62, 0x6A, 0x01)
local confluent_magic = string.char(0)
---generate_random_avro_file_name - method, that generates a random Avro file name to write it on the filesystem.
---@return string - file name, with prefix "tarantool_avro", and extension ".avro".
local function generate_random_avro_file_name()
    local random_number
    local random_string
    random_string = ""
    for _ = 1, 10, 1 do
        random_number = math.random(65, 90)
        random_string = random_string .. string.char(random_number)
    end
    return 'tarantool_avro' .. random_string .. '.avro'
end

---compile_avro_schema - method, that compiles JSON-string to Avro schema object.
---@param schema string - Avro schema in JSON-string.
---@return boolean,userdata|string - true|compiled Avro schema if process finished without error, else false|error.
local function compile_avro_schema(schema)
    checks('string')
    local ok, binary_schema = pcall(avro.Schema.new, avro.Schema, schema)
    if ok then
        return ok, binary_schema
    else
        return ok, error_repository.get_error_code('AVRO_SCHEMA_002', { schema = schema, error = binary_schema })
    end
end

--TODO Recursive clean?
---clean_table_of_records_from_avro_types -  method, that clean input table from Avro union types.
--- For example: [{"f1" : 5, "f2": {"string" : "abc" }}] -> [{"f1": 5, "f2" : "abc"}].
--- Works only on 1 nested table.
---@param table table - table {{}}, that need to clean.
---@return table - table, without Avro types, or input table without changes.
local function clean_table_of_records_from_avro_types(table)
    if type(table) ~= 'table' then
        return table
    end

    if misc_utils.is_array(table) then
        for _, row in ipairs(table) do
            for elem, value in pairs(row) do
                if type(value) == 'table' then
                    if value['string'] ~= nil then
                        row[elem] = tostring(value['string'])

                    elseif value['int'] ~= nil then
                        row[elem] = tonumber(value['int'])

                    elseif value['long'] ~= nil then
                        row[elem] = tonumber(value['long'])

                    elseif value['float'] ~= nil then
                        row[elem] = tonumber(value['float'])

                    elseif value['double'] ~= nil then
                        row[elem] = tostring(value['double'])

                    elseif value['boolean'] ~= nil then
                        row[elem] = value['boolean']
                    end
                end

            end
        end
    else
        for elem, value in pairs(table) do
            if type(value) == 'table' then
                if value['string'] ~= nil then
                    table[elem] = tostring(value['string'])

                elseif value['int'] ~= nil then
                    table[elem] = tonumber(value['int'])

                elseif value['long'] ~= nil then
                    table[elem] = tonumber(value['long'])

                elseif value['float'] ~= nil then
                    table[elem] = tonumber(value['float'])

                elseif value['double'] ~= nil then
                    table[elem] = tostring(value['double'])

                elseif value['boolean'] ~= nil then
                    table[elem] = value['boolean']
                end
            end
        end
    end

    return table
end

---encode_table_of_records_to_avro - method, that encode table [{},{}, ..., {}] to avro binary.
---@param schema string - JSON-string, that contains avro schema { type: "array" ....}.
---@param table table - table [{},{}, ..., {}] to encode.
---@return string - avro binary string.
local function encode_table_of_records_to_avro(schema, table)
    checks('string', 'table')
    local is_schema_compiled, compiled_schema = compile_avro_schema(schema)

    if is_schema_compiled == false then
        return false, compiled_schema
    end

    local is_wrapper_class_ok, wrapper_class = pcall(compiled_schema.wrapper_class, compiled_schema)

    if not is_wrapper_class_ok then
        compiled_schema:release()
        return false, wrapper_class
    end

    local is_wrapper_ok, wrapper = pcall(wrapper_class.new, wrapper_class)

    if not is_wrapper_ok then
        compiled_schema:release()
        return false, wrapper
    end

    local is_raw_value_ok, raw_value = pcall(compiled_schema.new_raw_value, compiled_schema)

    if not is_raw_value_ok then
        wrapper:release()
        compiled_schema:release()
        return false, raw_value
    end

    local is_value_wrapped, value_wrapped_error = pcall(wrapper.wrap, wrapper, raw_value)

    if not is_value_wrapped then
        wrapper:release()
        raw_value:release()
        compiled_schema:release()
        return false, value_wrapped_error
    end

    local is_value_filled, value_filled_error = pcall(wrapper.fill_from, wrapper, table)

    if not is_value_filled then
        wrapper:release()
        raw_value:release()
        compiled_schema:release()
        return false, value_filled_error
    end

    local is_result_encoded, result = pcall(raw_value.encode, raw_value)

    if not is_result_encoded then
        wrapper:release()
        raw_value:release()
        compiled_schema:release()
        return false, result
    end

    raw_value:release()
    wrapper:release()
    compiled_schema:release()

    return true, result
end

---encode_table_of_records_to_avro_object_container - method, that encode table [{},{}, ..., {}]
---to avro binary object container with schema.
---@param schema string - JSON-string, that contains avro schema { type: "record" ....}.
---@param table userdata -  table [{},{}, ..., {}] to encode.
---@param[opt="/dev/shm/"]  dir_to_safe string - optional parameter,
---that contains location on the filesystem to save intermediate results.
---@return string -  avro binary string.
local function encode_table_of_records_to_avro_object_container(schema, table, dir_to_safe)
    checks('string', 'table', '?string')

    local is_schema_compiled, compiled_schema = compile_avro_schema(schema)

    if is_schema_compiled == false then
        return false, compiled_schema
    end

    if dir_to_safe == nil then
        dir_to_safe = "/dev/shm/"
    end
    --check dir
    if not fio.path.exists(dir_to_safe) then
        compiled_schema:release()
        return false, dir_to_safe .. ' dir does not exists'
    end

    local file_name = dir_to_safe .. generate_random_avro_file_name()

    local is_writer_created, writer = pcall(avro.open, file_name, "w", compiled_schema)

    if not is_writer_created then
        return false, writer
    end

    local is_raw_value_ok, raw_value = pcall(compiled_schema.new_raw_value, compiled_schema)

    if not is_raw_value_ok then
        writer:close()
        compiled_schema:release()
        return false, raw_value
    end

    local is_data_encoded, data_encoding_error = pcall(
            function()
                for _, row in ipairs(table) do
                    raw_value:set_from_ast(row)
                    writer:write_raw(raw_value)
                end
            end)

    if not is_data_encoded then
        writer:close()
        raw_value:release()
        compiled_schema:release()
        return false, data_encoding_error
    end

    writer:close()
    raw_value:release()
    compiled_schema:release()

    local is_result_readed, result = pcall(file_utils.read_file, file_name)

    if not is_result_readed then
        local is_file_deleted, del = file_utils.delete_file(file_name)

        if not is_file_deleted then
            return false, del
        end

        return false, result
    end

    local is_file_deleted, del = file_utils.delete_file(file_name)

    if not is_file_deleted then
        return false, del
    end

    return true, result
end

---decode_avro_into_raw_value - method, that decode encoded avro value to raw_value wrapper.
---@param compiled_schema table - compiled avro schema.
---@param value string - encoded avro value.
---@return boolean|userdata - true|raw_value_wrapper if process finished without errors, else false|error.
local function decode_avro_into_raw_value(compiled_schema, value)
    checks('table', 'string')

    local is_raw_actual_created, raw_actual = pcall(compiled_schema.new_raw_value, compiled_schema)

    if is_raw_actual_created == false then
        return is_raw_actual_created, error_repository.get_error_code('AVRO_BINARY_DECODE_001',
                { schema = compiled_schema.to_json(), value = value, error = raw_actual })
    end

    local is_resolver_created, resolver = pcall(avro.ResolvedWriter, compiled_schema, compiled_schema)

    if is_resolver_created == false then
        raw_actual:release()
        return is_resolver_created, error_repository.get_error_code('AVRO_BINARY_DECODE_001',
                { schema = compiled_schema.to_json(), value = value, error = resolver })
    end

    local is_value_decoded, value_decode_error = pcall(resolver.decode, resolver, value, raw_actual)

    if is_value_decoded == false then
        raw_actual:release()
        return is_value_decoded, error_repository.get_error_code('AVRO_BINARY_DECODE_001',
                { schema = compiled_schema.to_json(), value = value, error = value_decode_error })
    end

    return true, raw_actual

end

---decode_single_object_avro - method, that decode single object avro to lua table.
---@param compiled_schema table - compiled avro schema.
---@param value string - encoded avro value.
---@return boolean|table - true|{value=...,size=decoded_size} if process finished without errors, else false|error.
local function decode_single_object_avro(compiled_schema, value)
    checks('table', 'string')

    local is_raw_decoded, raw_actual = decode_avro_into_raw_value(compiled_schema, value)

    if is_raw_decoded == false then
        compiled_schema:release()
        return false, raw_actual
    end

    local encoded_size = tonumber(raw_actual:encoded_size())

    local is_decode_json_string_obtained, decoded_json_string = pcall(raw_actual.to_json, raw_actual)

    if not is_decode_json_string_obtained then
        raw_actual:release()
        return is_decode_json_string_obtained, error_repository.get_error_code('AVRO_BINARY_DECODE_001',
                { schema = compiled_schema:to_json(), error = decoded_json_string })
    end

    local is_json_valid, decoded_json = pcall(json.decode, decoded_json_string)

    if not is_json_valid then
        return is_json_valid, error_repository.get_error_code('AVRO_BINARY_DECODE_001',
                { json_string = decoded_json_string,
                  schema = compiled_schema:to_json(), error = decoded_json })
    end

    local decoded_value = clean_table_of_records_from_avro_types(decoded_json)
    raw_actual:release()
    compiled_schema:release()

    return true, { ['value'] = decoded_value, ['size'] = encoded_size }
end

local function decode_avro_long_size(compiled_schema, value)
    local is_raw_decoded, raw_actual = decode_avro_into_raw_value(compiled_schema, value)

    if is_raw_decoded == false then
        return false, raw_actual
    end

    local value_decoded = tonumber(raw_actual:get())
    local size = tonumber(raw_actual:encoded_size())

    raw_actual:release()
    return true, { ['value'] = value_decoded, ['size'] = size }

end

---decode_data_from_blocks - method, that decodes blocks of data in encoded Avro object container.
---@param schema string - JSON-string with Avro schema.
---@param value string - binary string, that contains blocks of avro object container.
---@return boolean|table - true|{data=...,block_count=n} if process finished without errors, else false|error.
local function decode_data_from_blocks(schema, value)
    checks('string', 'string')
    --[[
    A file data block consists of:

        A long indicating the count of objects in this block.
        A long indicating the size in bytes of the serialized objects in the current block, after any codec is applied
        The serialized objects. If a codec is specified, this is compressed by that codec.
        The file's 16-byte sync marker.
    ]]
    local size_of_value = string.len(value)
    local size_of_decoded = 0
    local result = {}
    local block_count = 0

    local schema_long = [[{"type": "long"}]]

    local is_schema_compiled_long, compiled_schema_long = compile_avro_schema(schema_long)

    if is_schema_compiled_long == false then
        return false, compiled_schema_long
    end

    local is_schema_compiled, compiled_schema = compile_avro_schema(schema)

    if is_schema_compiled == false then
        return false, compiled_schema
    end

    while size_of_decoded < size_of_value do

        --- decode long indicating the count of objects in this block.
        local ok1, l1 = decode_avro_long_size(compiled_schema_long, value)

        if not ok1 then
            return false, l1
        end

        size_of_decoded = size_of_decoded + l1.size

        ---- decode long indicating the size in bytes of
        --- the serialized objects in the current block, after any codec is applied
        local ok2, l2 = decode_avro_long_size(compiled_schema_long, value:sub(1 + l1.size))

        if not ok2 then
            return false, l2
        end

        size_of_decoded = size_of_decoded + l2.size
        ---- decode block of data
        local block_result = {}
        local shift_to_data = 1 + l1.size + l2.size
        local shift = 0
        local i = 1

        while i <= l1.value do
            local ok3, data = decode_single_object_avro(
                    compiled_schema,
                    value:sub(shift_to_data + shift, shift_to_data + l2.value))
            shift = shift + data.size
            if not ok3 then
                return false, data
            end
            table.insert(block_result, data.value)

            i = i + 1
        end

        size_of_decoded = size_of_decoded + l2.value + sync_size
        if l1.value == 1 then
            --TODO array of arrays?
            result = block_result
        else
            table.insert(result, block_result)
        end
        block_count = block_count + 1

    end
    compiled_schema_long:release()
    compiled_schema:release()
    return true, { ['data'] = result, block_count = block_count }
end


---extract_metadata - method, that extract avro metadata from avro object container.
---@param value string - binary string, that contains encoded avro object container.
---@return boolean|table - true|{value=...,size=decoded_size} if process finished without errors, else false|error.
local function extract_metadata(value)
    checks('string')

    local is_avro = value:sub(1, 4) == magic

    if not is_avro then
        return false, error_repository.get_error_code('AVRO_BINARY_DECODE_003', { value = value }) --TODO Not avro
    end

    --[[
    A file header consists of:

        Four bytes, ASCII 'O', 'b', 'j', followed by 1.
        file metadata, including the schema.
        The 16-byte, randomly-generated sync marker for this file.
    ]]
    local schema = [[{"type": "map", "values": "bytes"}]]

    local is_schema_compiled, compiled_schema = compile_avro_schema(schema)

    if is_schema_compiled == false then
        return false, compiled_schema
    end

    local is_schema_decoded, metadata = decode_single_object_avro(compiled_schema, value:sub(string.len(magic) + 1))

    if not is_schema_decoded then
        return false, error_repository.get_error_code('AVRO_BINARY_DECODE_002',
                { schema = schema, error = metadata }) --TODO Not valid schema
    end

    compiled_schema:release()
    return true, metadata

end


---decode_object_container_avro - method, that decode avro object container with lua string decoding.
---@param value string -  binary string, that contains encoded avro object container.
---@param input_schema string|nil - optional JSON-string, that contains avro schema.
---@return boolean|table - true|table if process finished without errors, else false|error.
local function decode_object_container_avro(value, input_schema)
    checks('string', '?string')

    local is_schema_decoded, metadata = extract_metadata(value)

    if not is_schema_decoded then
        return false, metadata
    end

    local is_data_decoded, decoded_data

    if input_schema ~= nil then
        is_data_decoded, decoded_data = decode_data_from_blocks(input_schema,
                value:sub(string.len(magic) + 1 + metadata.size + sync_size))
    else
        is_data_decoded, decoded_data = decode_data_from_blocks(metadata.value["avro.schema"],
                value:sub(string.len(magic) + 1 + metadata.size + sync_size))
    end

    if not is_data_decoded then
        return false, error_repository.get_error_code('AVRO_BINARY_DECODE_002',
                { schema = metadata.value["avro.schema"],
                  error = decoded_data }) --TODO Not valid schema
    end

    if decoded_data.block_count == 1 then
        return true, clean_table_of_records_from_avro_types(decoded_data.data[1])
    else
        local result
        for _, v in ipairs(decoded_data.data) do
            if result == nil then
                result = v
            else
                misc_utils.append_table(result, v)
            end

        end
        return true, clean_table_of_records_from_avro_types(result)
    end

end

---transform_avro_binary_to_shm_file - support method, that saves avro binary value to file on filesystem.
---@param value string - binary string, that contains avro binary value.
---@param[opt="/dev/shm/"]  dir_to_safe string - optional parameter,
---that contains location on the filesystem to save intermediate results.
---@return boolean|string - true|filename if process finished without errors, false|error otherwise.
local function transform_avro_binary_to_shm_file(value, dir_to_safe)
    checks('string', '?string')

    if dir_to_safe == nil then
        dir_to_safe = "/dev/shm/"
    end
    --check dir
    if not fio.path.exists(dir_to_safe) then
        return false, dir_to_safe .. ' dir does not exists'
    end

    local file_name = dir_to_safe .. generate_random_avro_file_name()

    local res, err = file_utils.write_file(file_name, value, { "O_RDWR", "O_CREAT" },
            { "S_IRUSR", "S_IWUSR" })

    if not res then
        return false, err
    end

    return true, file_name
end

---decode_records_object_container_avro_fast - method, that decodes avro object container using C-wrapper.
---@param value string - binary string, that contains avro binary value.
---@param[opt="/dev/shm/"]  dir_to_safe string - optional parameter,
--- that contains location on the filesystem to save intermediate results.
---@return boolean|table - true|table if process finished without errors, false|error otherwise.
local function decode_records_object_container_avro_fast(value, dir_to_safe)
    checks('string', '?string')
    local try_count = 1
    :: restart ::

    local is_file_created, file = transform_avro_binary_to_shm_file(value, dir_to_safe)

    if not is_file_created then
        if try_count > 5 then
            return false, file
        end
        try_count = try_count + 1
        goto restart
    end

    local is_reader_created, reader = pcall(avro.open, file)
    if not is_reader_created then
        local is_file_deleted, del = file_utils.delete_file(file)

        if not is_file_deleted then
            return false, del
        end

        return false, reader
    end

    local blocks_result = {}

    local raw_value = reader:read_raw()

    while raw_value do
        table.insert(blocks_result, raw_value:to_json())
        raw_value:release()
        raw_value = reader:read_raw() --TODO check return value
    end

    reader:close()
    local is_file_deleted, del = file_utils.delete_file(file)

    if not is_file_deleted then
        return false, del
    end

    if #blocks_result == 1 and type(blocks_result[1]) ~= 'table' then
        return true, clean_table_of_records_from_avro_types(json.decode(blocks_result[1]))
    elseif #blocks_result > 1 and type(blocks_result[1]) == 'table' then
        local result
        for _, v in ipairs(blocks_result) do
            if result == nil then
                result = clean_table_of_records_from_avro_types(json.decode(v))
            else
                misc_utils.append_table(result, clean_table_of_records_from_avro_types(json.decode(v)))
            end

        end
        return true, result  -- for array of record
    else

        local result = {}
        for _, v in ipairs(blocks_result) do
            table.insert(result, json.decode(v))
        end
        return true, clean_table_of_records_from_avro_types(result)
    end

end

local function decode_records_object_container_avro_memory(value)
    checks('string')
    
    local is_reader_created, reader = pcall(avro.open, value, "m")
    if not is_reader_created then
        return false, reader
    end

    local blocks_result = {}

    local raw_value = reader:read_raw()

    while raw_value do
        table.insert(blocks_result, raw_value:to_table())
        raw_value:release()
        raw_value = reader:read_raw() --TODO check return value
    end
    reader:close()

    -- for arrays
    if #blocks_result == 1 then
        return true, blocks_result[1]
    end

    return true, blocks_result
end


---decode - method, that decodes avro object container using C-wrapper.
---@param value string - binary string, that contains avro binary value.
---@param schema string - Optional avro schema in JSON-string.
---@return boolean|table - true|table if process finished without errors, false|error otherwise.
local function decode(data,schema)
    checks('string','?string')

    local is_confluent_avro = false
    local confluent_schema_id
    --check for wire format
    --https://docs.confluent.io/current/schema-registry/serdes-develop/index.html#wire-format
    if data:sub(1,1) == confluent_magic then
        is_confluent_avro = true
        confluent_schema_id = data:sub(2,4)
    end

    if schema == nil then
        return decode_records_object_container_avro_memory(data)
    else
        local is_schema_compiled, compiled_schema = compile_avro_schema(schema)

        if is_schema_compiled == false then
            return false, compiled_schema
        end


        local res,err
        if is_confluent_avro then
            res,err = decode_single_object_avro(compiled_schema,data:sub(6))
        else
            res,err = decode_single_object_avro(compiled_schema,data)
        end

        if not res then
            compiled_schema:release()
            return false,err
        end

        compiled_schema:release()

        return res,err.value
    end
end

---encode - method, that encode table [{},{}, ..., {}]
---to avro binary object.
---@param schema string - JSON-string, that contains avro schema { type: "record" ....}.
---@param table userdata -  table [{},{}, ..., {}] to encode.
---@param is_object_container boolean - decode to object container?
---@return boolean|string - true|string if process finished without errors, false|error otherwise.
local function encode(schema,data,is_object_container)
    checks('string','table','boolean')
    if is_object_container then
        return encode_table_of_records_to_avro_object_container(schema,data)
    else return encode_table_of_records_to_avro(schema,data)
    end
end

return {
    compile_avro_schema = compile_avro_schema,
    extract_metadata = extract_metadata,
    clean_table_of_records_from_avro_types = clean_table_of_records_from_avro_types,
    decode = decode,
    encode = encode,
}