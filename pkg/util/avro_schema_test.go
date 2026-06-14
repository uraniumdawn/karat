// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package util

import "testing"

const catalogSchema = `{
    "type": "record",
    "name": "Catalog",
    "namespace": "com.example.app",
    "fields": [
        {"name": "ref_id", "type": "long"},
        {
            "name": "items",
            "type": {
                "type": "array",
                "items": {
                    "type": "record",
                    "name": "Item",
                    "fields": [
                        {"name": "item_id", "type": "long"},
                        {
                            "name": "attributes",
                            "type": ["null", {
                                "type": "array",
                                "items": {
                                    "type": "record",
                                    "name": "Attribute",
                                    "fields": [
                                        {"name": "attribute_id", "type": "long"},
                                        {"name": "label_a", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
                                        {"name": "label_b", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
                                        {"name": "ref_id", "type": ["null", "long"], "default": null},
                                        {
                                            "name": "detail",
                                            "type": ["null", "boolean", "double", {
                                                "type": "record",
                                                "name": "Outcome",
                                                "fields": [
                                                    {"name": "detail_id", "type": "long"},
                                                    {"name": "detail_label_a", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
                                                    {"name": "detail_label_b", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
                                                    {"name": "parent_ref", "type": ["null", "long"], "default": null},
                                                    {"name": "parent_label_a", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
                                                    {"name": "parent_label_b", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null}
                                                ]
                                            }],
                                            "default": null
                                        }
                                    ]
                                }
                            }],
                            "default": null
                        },
                        {"name": "price", "type": ["null", "long"], "default": null},
                        {"name": "rank_score", "type": ["null", "double"], "default": null}
                    ]
                }
            }
        }
    ]
}`

const catalogExpected = `Catalog:record:com.example.app
    ref_id:long
    items:array
        Item:record
            item_id:long
            attributes:union:null
                null
                array
                    Attribute:record
                        attribute_id:long
                        label_a:union:null
                            null
                            string
                        label_b:union:null
                            null
                            string
                        ref_id:union:null
                            null
                            long
                        detail:union:null
                            null
                            boolean
                            double
                            Outcome:record
                                detail_id:long
                                detail_label_a:union:null
                                    null
                                    string
                                detail_label_b:union:null
                                    null
                                    string
                                parent_ref:union:null
                                    null
                                    long
                                parent_label_a:union:null
                                    null
                                    string
                                parent_label_b:union:null
                                    null
                                    string
            price:union:null
                null
                long
            rank_score:union:null
                null
                double`

const entitySchema = `{
    "type": "record",
    "name": "Entity",
    "namespace": "com.example.app.model",
    "fields": [
        {"name": "id", "type": "long"},
        {
            "name": "kind",
            "type": {"type": "enum", "name": "Kind", "symbols": ["PRIMARY", "REGION_A", "REGION_B"]}
        },
        {
            "name": "path",
            "type": {
                "type": "array",
                "items": ["null", {"type": "string", "avro.java.string": "String"}]
            },
            "default": []
        },
        {"name": "left_key", "type": ["null", "long"], "default": null},
        {"name": "right_key", "type": ["null", "long"], "default": null},
        {"name": "title", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
        {
            "name": "status",
            "type": ["null", {
                "type": "enum",
                "name": "StatusKind",
                "symbols": ["ACTIVE", "LOCKED", "HIDDEN", "HIDDEN_AUTOMATICALLY"],
                "default": "ACTIVE"
            }],
            "default": null
        },
        {"name": "name", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
        {"name": "is_deleted", "type": ["null", "boolean"], "default": null}
    ]
}`

const entityExpected = `Entity:record:com.example.app.model
    id:long
    kind:enum
        PRIMARY
        REGION_A
        REGION_B
    path:array:[]
        union
            null
            string
    left_key:union:null
        null
        long
    right_key:union:null
        null
        long
    title:union:null
        null
        string
    status:union:null
        null
        StatusKind:enum:ACTIVE
            ACTIVE
            LOCKED
            HIDDEN
            HIDDEN_AUTOMATICALLY
    name:union:null
        null
        string
    is_deleted:union:null
        null
        boolean`

const aggregateSchema = `{
    "type": "record",
    "name": "Aggregate",
    "namespace": "com.example.app.intermediate",
    "fields": [
        {"name": "entityId", "type": ["null", "long"], "default": null},
        {
            "name": "primary",
            "type": {
                "type": "record",
                "name": "Primary",
                "fields": [
                    {
                        "name": "main",
                        "type": ["null", {
                            "type": "record",
                            "name": "Component",
                            "fields": [
                                {"name": "entityId", "type": "long"},
                                {"name": "attributeId", "type": "long"},
                                {
                                    "name": "kind",
                                    "type": {"type": "enum", "name": "ComponentKind", "symbols": ["BOOL", "NUMBER", "TEXT", "TEXT_A", "TEXT_B", "MULTI_VALUE"]}
                                },
                                {"name": "detailId", "type": ["null", "long"], "default": null},
                                {"name": "detail", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
                                {"name": "isDeleted", "type": ["null", "boolean"], "default": null}
                            ]
                        }],
                        "default": null
                    },
                    {"name": "altA", "type": ["null", "Component"], "default": null},
                    {"name": "altB", "type": ["null", "Component"], "default": null}
                ]
            }
        },
        {
            "name": "secondary",
            "type": {
                "type": "record",
                "name": "Secondary",
                "fields": [
                    {
                        "name": "entry",
                        "type": {
                            "type": "record",
                            "name": "Entry",
                            "fields": [
                                {"name": "id", "type": "long"},
                                {"name": "groupId", "type": ["null", "long"], "default": null},
                                {
                                    "name": "filterKind",
                                    "type": ["null", {"type": "enum", "name": "FilterKind", "symbols": ["EXCLUDING", "SUPPLEMENTING", "EMPTY"]}],
                                    "default": null
                                },
                                {
                                    "name": "state",
                                    "type": ["null", {"type": "enum", "name": "EntryState", "symbols": ["ACTIVE", "LOCKED"]}],
                                    "default": null
                                },
                                {"name": "isDeleted", "type": ["null", "boolean"], "default": null}
                            ]
                        }
                    },
                    {
                        "name": "settings",
                        "type": {
                            "type": "array",
                            "items": {
                                "type": "record",
                                "name": "Setting",
                                "fields": [
                                    {"name": "settingId", "type": "long"},
                                    {"name": "attributeId", "type": ["null", "long"], "default": null},
                                    {"name": "groupId", "type": ["null", "long"], "default": null},
                                    {"name": "isSearchable", "type": ["null", "boolean"], "default": null},
                                    {"name": "comparable", "type": ["null", {"type": "string", "avro.java.string": "String"}], "default": null},
                                    {
                                        "name": "status",
                                        "type": ["null", {"type": "enum", "name": "SettingStatus", "symbols": ["ACTIVE", "CALL_CENTER", "FACET", "LOCKED", "PASSIVE"]}],
                                        "default": null
                                    },
                                    {"name": "isDeleted", "type": ["null", "boolean"], "default": null}
                                ]
                            }
                        }
                    }
                ]
            }
        },
        {
            "name": "summary",
            "type": ["null", {
                "type": "record",
                "name": "Summary",
                "fields": [
                    {"name": "id", "type": "long"},
                    {"name": "parentRef", "type": ["null", "long"], "default": null},
                    {"name": "isDeleted", "type": ["null", "boolean"], "default": null}
                ]
            }],
            "default": null
        },
        {"name": "summaryParent", "type": ["null", "Summary"], "default": null}
    ]
}`

const aggregateExpected = `Aggregate:record:com.example.app.intermediate
    entityId:union:null
        null
        long
    primary:record
        main:union:null
            null
            Component:record
                entityId:long
                attributeId:long
                kind:enum
                    BOOL
                    NUMBER
                    TEXT
                    TEXT_A
                    TEXT_B
                    MULTI_VALUE
                detailId:union:null
                    null
                    long
                detail:union:null
                    null
                    string
                isDeleted:union:null
                    null
                    boolean
        altA:union:null
            null
            Component:record
        altB:union:null
            null
            Component:record
    secondary:record
        entry:record
            id:long
            groupId:union:null
                null
                long
            filterKind:union:null
                null
                FilterKind:enum
                    EXCLUDING
                    SUPPLEMENTING
                    EMPTY
            state:union:null
                null
                EntryState:enum
                    ACTIVE
                    LOCKED
            isDeleted:union:null
                null
                boolean
        settings:array
            Setting:record
                settingId:long
                attributeId:union:null
                    null
                    long
                groupId:union:null
                    null
                    long
                isSearchable:union:null
                    null
                    boolean
                comparable:union:null
                    null
                    string
                status:union:null
                    null
                    SettingStatus:enum
                        ACTIVE
                        CALL_CENTER
                        FACET
                        LOCKED
                        PASSIVE
                isDeleted:union:null
                    null
                    boolean
    summary:union:null
        null
        Summary:record
            id:long
            parentRef:union:null
                null
                long
            isDeleted:union:null
                null
                boolean
    summaryParent:union:null
        null
        Summary:record`

const mapAndLogicalTypesSchema = `{
    "type": "record",
    "name": "Order",
    "namespace": "com.example",
    "fields": [
        {"name": "attributes", "type": {"type": "map", "values": "string"}, "default": {}},
        {"name": "createdAt", "type": {"type": "long", "logicalType": "timestamp-millis"}},
        {"name": "price", "type": {"type": "bytes", "logicalType": "decimal", "precision": 10, "scale": 2}, "default": " "},
        {"name": "id", "type": {"type": "string", "logicalType": "uuid"}}
    ]
}`

const mapAndLogicalTypesExpected = "Order:record:com.example\n" +
	"    attributes:map:{}\n" +
	"        string\n" +
	"    createdAt:long(timestamp-millis)\n" +
	"    price:bytes(decimal(10,2)): \n" +
	"    id:string(uuid)"

func TestFormatAvroSchemaKarat(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   string
	}{
		{"Catalog", catalogSchema, catalogExpected},
		{"Entity", entitySchema, entityExpected},
		{"Aggregate", aggregateSchema, aggregateExpected},
		{"MapAndLogicalTypes", mapAndLogicalTypesSchema, mapAndLogicalTypesExpected},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FormatAvroSchemaKarat(tt.schema)
			if err != nil {
				t.Fatalf("FormatAvroSchemaKarat() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("FormatAvroSchemaKarat() =\n%s\nwant:\n%s", got, tt.want)
			}
		})
	}
}

func TestFormatAvroSchemaKarat_InvalidJSON(t *testing.T) {
	if _, err := FormatAvroSchemaKarat("not json"); err == nil {
		t.Fatal("expected error for invalid JSON, got nil")
	}
}
