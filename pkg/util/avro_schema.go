// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package util

import (
	"encoding/json"
	"fmt"
	"strings"
)

// primitiveAvroTypes are the Avro primitive type names that never carry their own
// nested children.
var primitiveAvroTypes = map[string]bool{
	"null":    true,
	"boolean": true,
	"int":     true,
	"long":    true,
	"float":   true,
	"double":  true,
	"bytes":   true,
	"string":  true,
}

// FormatAvroSchemaKarat renders an Avro schema JSON document in Karat's compact,
// hierarchical "<name>:<type>:<extra>" format: one line per field/branch/symbol, each
// indented 4 spaces deeper than its parent.
//
// Rules:
//   - record   -> <name>:record:<namespace>  (namespace only for the root record)
//   - array    -> <name>:array:<default>     (unnamed -> "array"); items follow, unnamed
//   - union    -> <name>:union:<default>     (unnamed -> "union"); branches follow, unnamed
//   - enum     -> <name>:enum:<extra>; symbols follow as plain values
//   - map      -> <name>:map:<default>       (unnamed -> "map"); values type follows, unnamed
//   - primitive -> <name>:<type>:<default>   (unnamed -> "<type>")
//   - logical type -> <name>:<logicalType>:<default>, underlying primitive dropped;
//     decimal shows precision/scale as "decimal(p,s)" or "decimal(p)"
//   - a bare type-name string referencing a previously-defined record/enum/fixed
//     renders as "<name>:<kind>" with no children
func FormatAvroSchemaKarat(schemaJSON string) (string, error) {
	dec := json.NewDecoder(strings.NewReader(schemaJSON))
	dec.UseNumber()

	var root any
	if err := dec.Decode(&root); err != nil {
		return "", fmt.Errorf("parse avro schema: %w", err)
	}

	var sb strings.Builder
	seen := make(map[string]string)
	renderAvroType(&sb, root, "", nil, false, 0, true, seen)

	return strings.TrimRight(sb.String(), "\n"), nil
}

// renderAvroType dispatches on the decoded JSON value's Go type and writes the
// rendered lines for fieldName (empty for unnamed/anonymous types, e.g. union
// branches, array items, map values) to sb.
func renderAvroType(
	sb *strings.Builder,
	t any,
	fieldName string,
	def any,
	hasDef bool,
	indent int,
	isRoot bool,
	seen map[string]string,
) {
	switch v := t.(type) {
	case string:
		renderNamedOrPrimitive(sb, v, fieldName, def, hasDef, indent, seen)
	case []any:
		renderUnion(sb, v, fieldName, def, hasDef, indent, seen)
	case map[string]any:
		renderComplex(sb, v, fieldName, def, hasDef, indent, isRoot, seen)
	}
}

// renderNamedOrPrimitive handles a bare type-name string: either an Avro primitive,
// or a reference to a previously-defined record/enum/fixed type.
func renderNamedOrPrimitive(
	sb *strings.Builder,
	typeName, fieldName string,
	def any,
	hasDef bool,
	indent int,
	seen map[string]string,
) {
	if primitiveAvroTypes[typeName] {
		if fieldName == "" {
			writeLine(sb, indent, typeName)
			return
		}
		writeLine(sb, indent, joinParts(fieldName, typeName, def, hasDef))
		return
	}

	// Reference to a previously-defined named type.
	kind, known := seen[typeName]
	if !known {
		// Unknown forward reference - fall back to showing the type name itself.
		if fieldName == "" {
			writeLine(sb, indent, typeName)
			return
		}
		writeLine(sb, indent, fieldName+":"+typeName)
		return
	}

	label := fieldName
	if label == "" {
		label = typeName
	}
	writeLine(sb, indent, label+":"+kind)
}

// renderUnion handles an Avro union (a JSON array of types).
func renderUnion(
	sb *strings.Builder,
	branches []any,
	fieldName string,
	def any,
	hasDef bool,
	indent int,
	seen map[string]string,
) {
	if fieldName == "" {
		writeLine(sb, indent, "union")
	} else {
		writeLine(sb, indent, joinParts(fieldName, "union", def, hasDef))
	}
	for _, branch := range branches {
		renderAvroType(sb, branch, "", nil, false, indent+1, false, seen)
	}
}

// renderComplex handles a JSON object type definition, dispatching on its "type" key.
func renderComplex(
	sb *strings.Builder,
	t map[string]any,
	fieldName string,
	def any,
	hasDef bool,
	indent int,
	isRoot bool,
	seen map[string]string,
) {
	typeName, _ := t["type"].(string)

	switch typeName {
	case "record":
		renderRecord(sb, t, fieldName, indent, isRoot, seen)
	case "enum":
		renderEnum(sb, t, fieldName, def, hasDef, indent, seen)
	case "array":
		renderArrayOrMap(sb, t, "array", "items", fieldName, def, hasDef, indent, seen)
	case "map":
		renderArrayOrMap(sb, t, "map", "values", fieldName, def, hasDef, indent, seen)
	case "fixed":
		renderFixed(sb, t, fieldName, def, hasDef, indent, seen)
	default:
		renderPrimitiveWithProps(sb, t, typeName, fieldName, def, hasDef, indent)
	}
}

// renderRecord handles a record type definition and recurses into its fields.
func renderRecord(
	sb *strings.Builder,
	t map[string]any,
	fieldName string,
	indent int,
	isRoot bool,
	seen map[string]string,
) {
	ownName, _ := t["name"].(string)

	label := fieldName
	if label == "" {
		label = ownName
	}

	line := label + ":record"
	if isRoot {
		if namespace, ok := t["namespace"].(string); ok && namespace != "" {
			line += ":" + namespace
		}
	}
	writeLine(sb, indent, line)

	if ownName != "" {
		seen[ownName] = "record"
	}

	fields, _ := t["fields"].([]any)
	for _, raw := range fields {
		field, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		name, _ := field["name"].(string)
		fieldDef, hasFieldDef := field["default"]
		renderAvroType(sb, field["type"], name, fieldDef, hasFieldDef, indent+1, false, seen)
	}
}

// renderEnum handles an enum type definition and lists its symbols.
func renderEnum(
	sb *strings.Builder,
	t map[string]any,
	fieldName string,
	fieldDef any,
	hasFieldDef bool,
	indent int,
	seen map[string]string,
) {
	ownName, _ := t["name"].(string)

	label := fieldName
	if label == "" {
		label = ownName
	}

	var extra any
	hasExtra := false
	if fieldName != "" && hasFieldDef {
		extra, hasExtra = fieldDef, true
	} else if enumDefault, ok := t["default"]; ok {
		extra, hasExtra = enumDefault, true
	}

	writeLine(sb, indent, joinParts(label, "enum", extra, hasExtra))

	if ownName != "" {
		seen[ownName] = "enum"
	}

	symbols, _ := t["symbols"].([]any)
	for _, raw := range symbols {
		if symbol, ok := raw.(string); ok {
			writeLine(sb, indent+1, symbol)
		}
	}
}

// renderArrayOrMap handles array and map type definitions, recursing into their single
// child type (items for array, values for map).
func renderArrayOrMap(
	sb *strings.Builder,
	t map[string]any,
	keyword, childKey string,
	fieldName string,
	def any,
	hasDef bool,
	indent int,
	seen map[string]string,
) {
	if fieldName == "" {
		writeLine(sb, indent, keyword)
	} else {
		writeLine(sb, indent, joinParts(fieldName, keyword, def, hasDef))
	}
	renderAvroType(sb, t[childKey], "", nil, false, indent+1, false, seen)
}

// renderFixed handles a fixed type definition (e.g. used by the "duration" and "uuid"
// logical types).
func renderFixed(
	sb *strings.Builder,
	t map[string]any,
	fieldName string,
	def any,
	hasDef bool,
	indent int,
	seen map[string]string,
) {
	ownName, _ := t["name"].(string)

	label := fieldName
	if label == "" {
		label = ownName
	}

	typeStr := "fixed"
	if size, ok := t["size"]; ok {
		typeStr = fmt.Sprintf("fixed(%v)", size)
	}
	typeStr = wrapLogicalType(typeStr, t)

	writeLine(sb, indent, joinParts(label, typeStr, def, hasDef))

	if ownName != "" {
		seen[ownName] = "fixed"
	}
}

// renderPrimitiveWithProps handles a JSON object whose "type" is an Avro primitive,
// optionally carrying a "logicalType" (and ignoring other props like
// "avro.java.string").
func renderPrimitiveWithProps(
	sb *strings.Builder,
	t map[string]any,
	underlying, fieldName string,
	def any,
	hasDef bool,
	indent int,
) {
	typeStr := wrapLogicalType(underlying, t)

	if fieldName == "" {
		writeLine(sb, indent, typeStr)
		return
	}
	writeLine(sb, indent, joinParts(fieldName, typeStr, def, hasDef))
}

// wrapLogicalType wraps underlying in "<underlying>(<logicalType>)" when t carries a
// "logicalType" attribute, otherwise returns underlying unchanged.
func wrapLogicalType(underlying string, t map[string]any) string {
	logicalType, ok := t["logicalType"].(string)
	if !ok {
		return underlying
	}
	return fmt.Sprintf("%s(%s)", underlying, renderLogicalTypeName(logicalType, t))
}

// renderLogicalTypeName returns the display name for a logical type, including
// precision/scale for "decimal".
func renderLogicalTypeName(logicalType string, t map[string]any) string {
	if logicalType != "decimal" {
		return logicalType
	}
	if scale, ok := t["scale"]; ok {
		return fmt.Sprintf("decimal(%v,%v)", t["precision"], scale)
	}
	return fmt.Sprintf("decimal(%v)", t["precision"])
}

// joinParts builds a "<name>:<type>" or "<name>:<type>:<default>" line.
func joinParts(name, typ string, def any, hasDef bool) string {
	s := name + ":" + typ
	if hasDef {
		s += ":" + formatDefault(def)
	}
	return s
}

// formatDefault renders an Avro default value for display.
func formatDefault(v any) string {
	switch val := v.(type) {
	case nil:
		return "null"
	case bool:
		if val {
			return "true"
		}
		return "false"
	case json.Number:
		return val.String()
	case string:
		return val
	case []any:
		if len(val) == 0 {
			return "[]"
		}
		b, _ := json.Marshal(val)
		return string(b)
	case map[string]any:
		if len(val) == 0 {
			return "{}"
		}
		b, _ := json.Marshal(val)
		return string(b)
	default:
		return fmt.Sprintf("%v", val)
	}
}

// writeLine writes one indented line (4 spaces per level) followed by a newline.
func writeLine(sb *strings.Builder, indent int, content string) {
	sb.WriteString(strings.Repeat("    ", indent))
	sb.WriteString(content)
	sb.WriteString("\n")
}
