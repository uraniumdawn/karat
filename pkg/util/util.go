// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

// Package util provides utility functions for the karat application.
package util

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"
	"text/template"

	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"

	"github.com/uraniumdawn/karat/pkg/config"
)

// ParseShellCommand expands {{bootstrap}} and {{topic}} placeholders in templateStr
// and splits the result into shell arguments, respecting quotes.
func ParseShellCommand(templateStr, topic, bootstrap string) ([]string, error) {
	// Replace user-friendly {{bootstrap}} and {{topic}} with Go template syntax
	templateStr = strings.ReplaceAll(templateStr, "{{bootstrap}}", "{{.bootstrap}}")
	templateStr = strings.ReplaceAll(templateStr, "{{topic}}", "{{.topic}}")

	tmpl, err := template.New("cmd").Parse(templateStr)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, map[string]string{
		"topic":     topic,
		"bootstrap": bootstrap,
	})
	if err != nil {
		return nil, err
	}

	expanded := buf.String()

	// Split into args with proper quote handling
	args, err := splitShellArgs(expanded)
	if err != nil {
		return nil, err
	}

	return args, nil
}

// splitShellArgs splits a command string into arguments, respecting quotes
func splitShellArgs(s string) ([]string, error) {
	var args []string
	var current strings.Builder
	var inSingleQuote, inDoubleQuote bool
	var escaped bool

	for i, r := range s {
		if escaped {
			current.WriteRune(r)
			escaped = false
			continue
		}

		switch r {
		case '\\':
			if inSingleQuote {
				// Backslash in single quotes is literal
				current.WriteRune(r)
			} else {
				// Mark next character as escaped
				escaped = true
			}
		case '\'':
			if inDoubleQuote {
				current.WriteRune(r)
			} else {
				inSingleQuote = !inSingleQuote
			}
		case '"':
			if inSingleQuote {
				current.WriteRune(r)
			} else {
				inDoubleQuote = !inDoubleQuote
			}
		case ' ', '\t', '\n':
			if inSingleQuote || inDoubleQuote {
				current.WriteRune(r)
			} else if current.Len() > 0 {
				args = append(args, current.String())
				current.Reset()
			}
		default:
			current.WriteRune(r)
		}

		// Check if we're at the end of the string
		if i == len(s)-1 && current.Len() > 0 {
			args = append(args, current.String())
		}
	}

	if inSingleQuote || inDoubleQuote {
		return nil, fmt.Errorf("unmatched quote in command")
	}

	return args, nil
}

// SetTableHeaders sets row 0 of table to non-selectable header cells with the given
// labelColor, one cell per entry in headers, in column order.
func SetTableHeaders(table *tview.Table, labelColor tcell.Color, headers ...string) {
	for col, text := range headers {
		table.SetCell(0, col, tview.NewTableCell(text).SetSelectable(false).SetTextColor(labelColor))
	}
}

// SetColumnMarker puts marker in column col of activeRow and clears that column in every
// other data row, leaving the header row (row 0) untouched. Used for single-choice marker
// columns, where at most one row can carry the marker. Cells in col are replaced, so any
// per-cell styling there is lost.
func SetColumnMarker(table *tview.Table, col, activeRow int, marker string) {
	for row := 1; row < table.GetRowCount(); row++ {
		text := ""
		if row == activeRow {
			text = marker
		}
		table.SetCell(row, col, tview.NewTableCell(text))
	}
}

// TableToCSV writes the contents of table to fileName in CSV format.
func TableToCSV(fileName string, table *tview.Table) {
	file, _ := os.Create(fileName)
	defer func() {
		_ = file.Close()
	}()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	rows, cols := table.GetRowCount(), table.GetColumnCount()
	for row := 0; row < rows; row++ {
		var record []string
		for col := 0; col < cols; col++ {
			cell := table.GetCell(row, col)
			if cell != nil {
				record = append(record, cell.Text)
			} else {
				record = append(record, "")
			}
		}
		_ = writer.Write(record)
	}
}

// NewModal centers p in a flex layout sized for a general-purpose modal.
func NewModal(p tview.Primitive) tview.Primitive {
	return tview.NewFlex().
		AddItem(nil, 0, 2, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 1, 0, false).
			AddItem(p, 0, 2, true).
			AddItem(nil, 0, 2, false), 0, 5, true).
		AddItem(nil, 2, 0, false)
}

// NewWideModal puts p near the top of the content area with a fixed height, spanning the
// width of a wide dialog.
func NewWideModal(p tview.Primitive, height int) tview.Primitive {
	return tview.NewFlex().
		AddItem(nil, 1, 0, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 1, 0, false).
			AddItem(p, height, 0, true).
			AddItem(nil, 0, 9, false), 0, 2, true).
		AddItem(nil, 1, 0, false)
}

// NewBottomModal anchors p to the bottom third of the content area, as wide as a
// confirmation dialog and with the same one-row margin, here below p instead of above it.
func NewBottomModal(p tview.Primitive) tview.Primitive {
	return tview.NewFlex().
		AddItem(nil, 1, 0, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 0, 2, false).
			AddItem(p, 0, 1, true).
			AddItem(nil, 1, 0, false), 0, 2, true).
		AddItem(nil, 1, 0, false)
}

// NewResourceModal centers p in a flex layout with a fixed height, sized for resource forms.
func NewResourceModal(p tview.Primitive, height int) tview.Primitive {
	return tview.NewFlex().
		AddItem(nil, 0, 2, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 1, 0, false).
			AddItem(p, height, 0, true).
			AddItem(nil, 0, 9, false), 0, 2, true).
		AddItem(nil, 2, 0, false)
}

// NewTopicModal centers p in a flex layout sized for topic creation/edit forms.
func NewTopicModal(p tview.Primitive) tview.Primitive {
	return tview.NewFlex().
		AddItem(nil, 0, 2, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 1, 0, false).
			AddItem(p, 0, 10, true).
			AddItem(nil, 0, 9, false), 0, 2, true).
		AddItem(nil, 2, 0, false)
}

// NewConnectorActionModal centers p in a flex layout sized for connector action dialogs.
func NewConnectorActionModal(p tview.Primitive) tview.Primitive {
	return tview.NewFlex().
		AddItem(nil, 0, 2, false).
		AddItem(tview.NewFlex().SetDirection(tview.FlexRow).
			AddItem(nil, 1, 0, false).
			AddItem(p, 3, 0, true).
			AddItem(nil, 0, 9, false), 0, 2, true).
		AddItem(nil, 2, 0, false)
}

// GetInt64 parses an int64 from an input field, returns -1 if empty or invalid.
func GetInt64(inputField *tview.InputField) int64 {
	text := inputField.GetText()
	if text == "" {
		return -1
	}
	res, _ := strconv.ParseInt(text, 10, 64)
	return res
}

// GetInt32 parses an int32 from an input field, returns -1 if empty or invalid.
func GetInt32(inputField *tview.InputField) int32 {
	text := inputField.GetText()
	if text == "" {
		return -1
	}
	res, _ := strconv.ParseInt(text, 10, 32)
	return int32(res)
}

// ToClustersMap converts a config cluster slice to a map keyed by name.
func ToClustersMap(cfg *config.Config) map[string]*config.ClusterConfig {
	clusterMap := make(map[string]*config.ClusterConfig)
	for _, cluster := range cfg.Karat.Clusters {
		clusterMap[cluster.Name] = cluster
	}
	return clusterMap
}

// ToSchemaRegistryMap converts a schema registry slice to a map keyed by name.
func ToSchemaRegistryMap(cfg *config.Config) map[string]*config.SchemaRegistryConfig {
	srMap := make(map[string]*config.SchemaRegistryConfig)
	for _, sr := range cfg.Karat.SchemaRegistries {
		srMap[sr.Name] = sr
	}
	return srMap
}

// ToConnectMap converts a connect slice to a map keyed by name.
func ToConnectMap(cfg *config.Config) map[string]*config.ConnectConfig {
	connectMap := make(map[string]*config.ConnectConfig)
	for _, connect := range cfg.Karat.Connect {
		connectMap[connect.Name] = connect
	}
	return connectMap
}

// BuildTitle creates a formatted title string from parts separated by colons.
func BuildTitle(parts ...string) string {
	var builder strings.Builder
	builder.WriteString(" ")
	for i, part := range parts {
		builder.WriteString(strings.ToLower(part))
		if i < len(parts)-1 {
			builder.WriteString(":")
		}
	}
	builder.WriteString(" ")
	return builder.String()
}

// BuildPageKey creates a page key string from parts separated by colons.
func BuildPageKey(parts ...string) string {
	var builder strings.Builder
	for i, part := range parts {
		builder.WriteString(strings.ToLower(part))
		if i < len(parts)-1 {
			builder.WriteString(":")
		}
	}
	return builder.String()
}

// BuildCliCommand expands {{bootstrap}}, {{topic}}, and {{srURL}} placeholders in templateStr.
func BuildCliCommand(templateStr, bootstrap, topic, schemaRegistryURL string) string {
	result := strings.ReplaceAll(templateStr, "{{bootstrap}}", bootstrap)
	result = strings.ReplaceAll(result, "{{topic}}", topic)
	result = strings.ReplaceAll(result, "{{srURL}}", schemaRegistryURL)
	return result
}

// SetSearchableTableTitle sets the title of a tview.Table with an optional filter.
func SetSearchableTableTitle(table *tview.Table, title, filter string) {
	if filter != "" {
		table.SetTitle(fmt.Sprintf("%s[grey]/%s ", title, filter))
	} else {
		table.SetTitle(title)
	}
}
