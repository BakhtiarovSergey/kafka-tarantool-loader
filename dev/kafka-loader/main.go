package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"strings"

	"github.com/linkedin/goavro"
)

const (
	_StagingTable = "adb__adg_test__env_test_staging"
	_ActualTable  = "adb__adg_test__env_test_actual"
	_HistoryTable = "adb__adg_test__env_test_history"
	_KafkaTopic   = "TEST"
)

type AdgClient struct {
	addr string
	c    *http.Client
}

func (a *AdgClient) send_request(method, url string, body io.Reader) error {
	req, err := http.NewRequest(method, a.addr+url, body)
	if err != nil {
		return err
	}

	resp, err := a.c.Do(req)
	if err != nil {
		return err
	}

	respBody, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	log.Println(string(respBody))
	return nil
}

func (a *AdgClient) create_table() error {
	test_yml_json := fmt.Sprintf(`{
		"spaces": {
			"%s": {
				"format": [
					{
						"name": "id",
						"type": "integer",
						"is_nullable": false
					},
					{
						"name": "bucket_id",
						"type": "unsigned",
						"is_nullable": false
					},
					{
						"name": "sys_from",
						"type": "number",
						"is_nullable": false
					},
					{
						"name": "sys_to",
						"type": "number",
						"is_nullable": true
					},
					{
						"name": "sys_op",
						"type": "number",
						"is_nullable": false
					},
					{
						"name": "gos_number",
						"type": "string",
						"is_nullable": true
					}
				],
				"temporary": false,
				"engine": "vinyl",
				"indexes": [
					{
						"unique": true,
						"parts": [
							{
								"path": "id",
								"type": "integer",
								"is_nullable": false
							},
							{
								"path": "sys_from",
								"type": "number",
								"is_nullable": false
							}
						],
						"type": "TREE",
						"name": "id"
					},
					{
						"unique": false,
						"parts": [
							{
								"path": "sys_from",
								"type": "number",
								"is_nullable": false
							}
						],
						"type": "TREE",
						"name": "x_sys_from"
					},
					{
						"unique": false,
						"parts": [
							{
								"path": "bucket_id",
								"type": "unsigned",
								"is_nullable": false
							}
						],
						"type": "TREE",
						"name": "bucket_id"
					}
				],
				"is_local": false,
				"sharding_key": [
					"id"
				]
			},
			"%s": {
				"format": [
					{
						"name": "id",
						"type": "integer",
						"is_nullable": false
					},
					{
						"name": "bucket_id",
						"type": "unsigned",
						"is_nullable": false
					},
					{
						"name": "sys_from",
						"type": "number",
						"is_nullable": false
					},
					{
						"name": "sys_to",
						"type": "number",
						"is_nullable": true
					},
					{
						"name": "sys_op",
						"type": "number",
						"is_nullable": false
					},
					{
						"name": "gos_number",
						"type": "string",
						"is_nullable": true
					}
				],
				"temporary": false,
				"engine": "vinyl",
				"indexes": [
					{
						"unique": true,
						"parts": [
							{
								"path": "id",
								"type": "integer",
								"is_nullable": false
							},
							{
								"path": "sys_from",
								"type": "number",
								"is_nullable": false
							}
						],
						"type": "TREE",
						"name": "id"
					},
					{
						"unique": false,
						"parts": [
							{
								"path": "sys_from",
								"type": "number",
								"is_nullable": false
							}
						],
						"type": "TREE",
						"name": "x_sys_from"
					},
					{
						"unique": false,
						"parts": [
							{
								"path": "sys_to",
								"type": "number",
								"is_nullable": true
							},
							{
								"path": "sys_op",
								"type": "number",
								"is_nullable": false
							}
						],
						"type": "TREE",
						"name": "x_sys_to"
					},
					{
						"unique": false,
						"parts": [
							{
								"path": "bucket_id",
								"type": "unsigned",
								"is_nullable": false
							}
						],
						"type": "TREE",
						"name": "bucket_id"
					}
				],
				"is_local": false,
				"sharding_key": [
					"id"
				]
			},
			"%s": {
				"format": [
					{
						"name": "id",
						"type": "integer",
						"is_nullable": false
					},
					{
						"name": "gos_number",
						"type": "string",
						"is_nullable": true
					},
					{
						"name": "sys_op",
						"type": "number",
						"is_nullable": false
					},
					{
						"name": "bucket_id",
						"type": "unsigned",
						"is_nullable": false
					}
				],
				"temporary": false,
				"engine": "vinyl",
				"indexes": [
					{
						"unique": true,
						"parts": [
							{
								"path": "id",
								"type": "integer",
								"is_nullable": false
							}
						],
						"type": "TREE",
						"name": "id"
					},
					{
						"unique": false,
						"parts": [
							{
								"path": "bucket_id",
								"type": "unsigned",
								"is_nullable": false
							}
						],
						"type": "TREE",
						"name": "bucket_id"
					}
				],
				"is_local": false,
				"sharding_key": [
					"id"
				]
			}
		}
	}
	`, _StagingTable, _ActualTable, _HistoryTable)

	return a.send_request(http.MethodPost, "/api/v1/ddl/table/queuedCreate", strings.NewReader(test_yml_json))
}

func (a *AdgClient) drop_table() error {
	resp := fmt.Sprintf(`{"tableList":["%s","%s","%s"]}`, _StagingTable, _ActualTable, _HistoryTable)
	return a.send_request(http.MethodDelete, "/api/v1/ddl/table/queuedDelete", strings.NewReader(resp))
}

func (a *AdgClient) insert_data() error {
	p := url.Values{}
	p.Add("_stage_data_table_name", _StagingTable)
	p.Set("_actual_data_table_name", _ActualTable)
	p.Add("_historical_data_table_name", _HistoryTable)
	p.Add("_delta_number", "0")
	p.Encode()

	return a.send_request(http.MethodGet, "/api/etl/transfer_data_to_scd_table?"+p.Encode(), nil)
}

func (a *AdgClient) load_data() error {
	resp := fmt.Sprintf(`{
		"topicName": "%s",
		"consumerGroupName": "tarantool",
		"spaceNames": [
			"%s"
		],
		"maxNumberOfMessagesPerPartition": 100,
		"avroSchema": {
			"type":"record",
			"name":"env_test",
			"fields":[
				{
					"name": "id", 
					"type": "long"
				},
				{
					"name": "gos_number",
					"type": "string"
				}
			]
		},
		"callbackFunction": {
			"callbackFunctionName": "transfer_data_to_scd_table_on_cluster_cb",
			"callbackFunctionParams": {
				"_space": "%s",
				"_stage_data_table_name": "%s",
				"_actual_data_table_name": "%s",
				"_historical_data_table_name": "%s",
				"_delta_number": 40
			},
			"maxNumberOfMessagesPerPartition": 200,
			"maxIdleSecondsBeforeCbCall": 100
		}
	}
	`, _KafkaTopic, _StagingTable, _StagingTable, _StagingTable, _ActualTable, _HistoryTable)
	return a.send_request(http.MethodPost, "/api/v1/kafka/subscription", strings.NewReader(resp))
}

// func (a *AdgClient) load_data() error {
// 	resp := fmt.Sprintf(`{"topicName":"%s","spaces":["%s","%s","%s"]}`, _KafkaTopic, _StagingTable, _ActualTable, _HistoryTable)
// 	return a.send_request(http.MethodPost, "/api/v1/kafka/dataload", strings.NewReader(resp))
// }

func main() {
	// c := AdgClient{
	// 	addr: "http://localhost:8081",
	// 	c:    &http.Client{},
	// }

	// CREATE TABLE operation
	// if err := c.create_table(); err != nil {
	// 	log.Fatal(err)
	// }

	// TODO: write kafka uploader

	// INSERT ... INTO
	// if err := c.load_data(); err != nil {
	// 	log.Fatal(err)
	// }

	// if err := c.insert_data(); err != nil {
	// 	log.Fatal(err)
	// }
	codec, err := goavro.NewCodec(`{
		"type":"record",
		"name":"env_test",
		"fields":[
			{
				"name": "id", 
				"type": "long"
			},
			{
				"name": "gos_number",
				"type": "string"
			}
		]
	}`)
	if err != nil {
		log.Fatal(err)
	}

	m := map[string]interface{}{
		"id":         1,
		"gos_number": "a777aa750",
	}

	binary, err := codec.BinaryFromNative(nil, m)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(binary)
	// DROP TABLE operation
	// if err := c.drop_table(); err != nil {
	// 	log.Fatal(err)
	// }

}
