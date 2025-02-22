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
--- Generated by EmmyLua(https://github.com/EmmyLua)
--- Created by ashitov.
--- DateTime: 8/4/20 12:27 PM
---
local fiber = require('fiber')
local log = require('log')
local checks = require('checks')
local clock = require('clock')

local kafka_msg_deserialize_transformation = {}
kafka_msg_deserialize_transformation.__index = kafka_msg_deserialize_transformation
kafka_msg_deserialize_transformation.__type = 'transformation'
kafka_msg_deserialize_transformation.__call = function (cls, ...)
    return cls.new(...)
end

local function enrich_msg_with_subscribe_metadata(msg,error_dest,fiber_name)

    local topic_name = msg.msg.topic

    if topic_name == nil then
        log.error("ERROR: Cannot find topic name in kafka message")
        if error_dest ~= nil then
            error_dest:put({msg = msg.msg,
                            err = "ERROR: Cannot find topic name in kafka message",
                            fiber = fiber_name,
                            timestamp = clock.time()})
        end
        return nil
    end

    local kafka_topic = box.space['_KAFKA_TOPIC']
    local kafka_topic_prev = box.space['_KAFKA_TOPIC_PREV']

    if kafka_topic == nil then
        log.error("ERROR: Cannot find _KAFKA_TOPIC space on app.roles.adg_kafka_connector")
        if error_dest ~= nil then
            error_dest:put({msg = msg.msg,
                            err = "ERROR: Cannot find _KAFKA_TOPIC space on app.roles.adg_kafka_connector",
                            fiber = fiber_name,
                            timestamp = clock.time()})
        end
        return nil
    end

    if kafka_topic_prev == nil then
        log.error("ERROR: Cannot find _KAFKA_TOPIC_PREV space on app.roles.adg_kafka_connector")
        if error_dest ~= nil then
            error_dest:put({msg = msg.msg,
                            err = "ERROR: Cannot find _KAFKA_TOPIC_PREV space on app.roles.adg_kafka_connector",
                            fiber = fiber_name,
                            timestamp = clock.time()})
        end
        return nil
    end

    local topic_info = kafka_topic:get(topic_name) or kafka_topic_prev:get(topic_name)

    if topic_info == nil then
        log.error(string.format("ERROR: Cannot find %s topic in list of subscribed topics", topic_name))
        if error_dest ~= nil then
            error_dest:put({msg = msg.msg,
                            err = string.format("ERROR: Cannot find %s topic in list of subscribed topics", topic_name),
                            fiber = fiber_name,
                            timestamp = clock.time()})
        end
        return nil
    end

    local enrich_msg = table.deepcopy(msg)

    enrich_msg.msg['spaces'] = topic_info['SPACE_NAMES']
    enrich_msg.msg['avro_schema'] = topic_info['AVRO_SCHEMA']
    enrich_msg.msg['cnt_cb_call'] = topic_info['MAX_NUMBER_OF_MESSAGES_PER_PARTITION_WITH_CB_CALL']
    enrich_msg.msg['sec_cb_call'] = topic_info['MAX_IDLE_SECONDS_BEFORE_CB_CALL'] or 10
    enrich_msg.msg['cb_func_name'] = topic_info['CALLBACK_FUNCTION_NAME']
    enrich_msg.msg['cb_func_params'] = topic_info['CALLBACK_FUNCTION_PARAMS']


    return enrich_msg

end

local function process(kafka_msg,error_dest,fiber_name)
    if kafka_msg == nil then
        return nil
    else
        local deserialize_msg = { msg = { topic = kafka_msg:topic(), partition = kafka_msg:partition(), offset = kafka_msg:offset(), key = kafka_msg:key()}, raw_msg = kafka_msg}
        local enrich_msg = enrich_msg_with_subscribe_metadata(deserialize_msg,error_dest,fiber_name)
        return enrich_msg
    end
end


local function create_fiber(self,process_function, fiber_name,source,error_dest)
    local process_fiber = fiber.new(
            function()
                while true do
                    if (source:is_closed() or self.process_channel:is_closed()) then
                        log.warn('WARN: Channels closed in fiber %s', fiber_name)
                        return
                    end
                    local input = source:get()
                    if input ~= nil then
                        local result = process_function(input,error_dest,fiber_name)
                        if result ~= nil then
                            local sent = self.process_channel:put(result)
                            if not sent then
                                log.error("ERROR: %s fiber send error",fiber_name)
                                if error_dest ~= nil then
                                    error_dest:put({msg = result.msg,
                                                    err = string.format("ERROR: %s fiber send error",fiber_name),
                                                    fiber = fiber_name,
                                                    timestamp = clock.time()})
                                end
                            end
                        end
                    end
                    fiber.yield()
                end
            end)
    process_fiber:name(fiber_name)
    return process_fiber
end

function kafka_msg_deserialize_transformation.init(channel_capacity,source,error_dest)
    local self = setmetatable({}, kafka_msg_deserialize_transformation)
    self.process_channel = fiber.channel(channel_capacity)
    self.process_fiber = create_fiber(self,process,"kafka_msg_deserialize_fiber",source,error_dest)
    return self
end

function kafka_msg_deserialize_transformation.get_result_channel(self)
    return self.process_channel
end

return kafka_msg_deserialize_transformation