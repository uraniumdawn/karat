// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"text/tabwriter"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gdamore/tcell/v2"
	"github.com/rivo/tview"
	"github.com/rs/zerolog/log"

	"github.com/uraniumdawn/karat/pkg/config"
	"github.com/uraniumdawn/karat/pkg/consumer"
	"github.com/uraniumdawn/karat/pkg/schemaregistry"
	"github.com/uraniumdawn/karat/pkg/util"
)

// StartConsuming opens a streaming output page and starts the native consumer.
// formatFn controls how each record is rendered; pass formatConsumeRecord for JSON output.
// filter, when non-empty, hides records whose formatted line does not contain the substring.
func (app *App) StartConsuming(
	topicName string,
	params consumer.Params,
	formatFn func(consumer.Record) string,
	filter string,
) {
	ctx, cancelFunc := context.WithCancel(context.Background())

	records := make(chan consumer.Record, 200)
	errs := make(chan error, 20)

	// No SetChangedFunc: every write to this view already happens inside a QueueUpdateDraw,
	// which redraws. A changed handler calling Draw would queue a second, redundant update per
	// record — tview runs the handler on a goroutine of its own, so a fast topic leaves a
	// goroutine parked in QueueUpdate for every record consumed.
	view := tview.NewTextView().
		SetTextAlign(tview.AlignLeft).
		SetDynamicColors(true).
		SetWrap(true).
		SetWordWrap(false).
		SetMaxLines(5000).
		SetScrollable(true)
	view.SetBorder(true).SetBorderPadding(0, 0, 1, 0)

	pageName := util.BuildPageKey(app.Selected.Cluster.Name, ConsumeOutput, topicName)
	app.AddToPagesRegistry(pageName, view, ConsumeOutputPageMenu, false)

	// Consuming is an operation in flight, so it takes the spinner and stands until it is
	// over: the record count sent when the drain goroutine finishes replaces it.
	SendStatusProgress(fmt.Sprintf("consuming '%s'", topicName))

	var recordCount int64
	var isActive int32 = 1
	spinnerIdx := 0

	// The statistics are written by the goroutines draining records and errors, and read by
	// the UI goroutine when <F2> opens the statistics modal, hence the mutex.
	partitionCounts := make(map[int32]int64)
	partitionFirstOffset := make(map[int32]int64)
	partitionLastOffset := make(map[int32]int64)
	var consumeErrors []string
	var statsMu sync.Mutex

	// statsSnapshot copies the statistics so the modal reads values the drain goroutines
	// cannot still be writing to.
	statsSnapshot := func() (counts, first, last map[int32]int64, errs []string) {
		statsMu.Lock()
		defer statsMu.Unlock()
		return maps.Clone(partitionCounts),
			maps.Clone(partitionFirstOffset),
			maps.Clone(partitionLastOffset),
			append([]string(nil), consumeErrors...)
	}

	// Spinner goroutine — updates title while consuming.
	go func() {
		ticker := time.NewTicker(80 * time.Millisecond)
		defer ticker.Stop()
		for range ticker.C {
			if atomic.LoadInt32(&isActive) == 0 {
				return
			}
			cnt := atomic.LoadInt64(&recordCount)
			frame := SpinnerFrames[spinnerIdx]
			spinnerIdx = (spinnerIdx + 1) % len(SpinnerFrames)
			app.QueueUpdateDraw(func() {
				view.SetTitle(fmt.Sprintf(" %s Consuming: %s [%d] ", frame, topicName, cnt))
			})
		}
	}()

	app.Layout.PagesRegistry.PageMenuMap[ConsumeStats] = ConsumeStatsPageMenu

	view.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if IsKey(event, 't') {
			if atomic.LoadInt32(&isActive) == 0 {
				SendStatusDone("consumer already stopped")
				return nil
			}
			cancelFunc()
			SendStatusProgress("stopping consumer…")
			return nil
		}
		if event.Key() == tcell.KeyF2 {
			if atomic.LoadInt32(&isActive) == 1 {
				SendStatusDone("consumer still active — press t to stop first")
				return nil
			}
			counts, first, last, errs := statsSnapshot()
			app.ConsumeStatsModal(topicName, counts, first, last, errs)
			app.ShowModalPage(ConsumeStats)
			return nil
		}
		// <x> closes the page, the same key the opened-pages modal uses. <C-d> is reserved for
		// deleting things in Kafka and must not also mean "close this".
		if IsKey(event, 'x') {
			if atomic.LoadInt32(&isActive) == 1 {
				SendStatusDone("consumer is still active — press t to stop first")
				return nil
			}
			app.RemoveFromPagesRegistry(pageName)
			return nil
		}
		return event
	})

	go consumer.Consume(ctx, params, records, errs)

	// Drain records; when the channel closes, finalize.
	go func() {
		defer func() {
			cancelFunc()
			atomic.StoreInt32(&isActive, 0)
			cnt := atomic.LoadInt64(&recordCount)
			app.QueueUpdateDraw(func() {
				view.SetTitle(fmt.Sprintf(" Consume: %s [%d records] ", topicName, cnt))
			})
			SendStatusDone(fmt.Sprintf("consumed %d records", cnt))
		}()
		for rec := range records {
			if ctx.Err() != nil {
				// Stop requested — drain the channel without further rendering so
				// Consume can close it and this goroutine can finalize promptly,
				// instead of working through a backlog of queued UI draws.
				continue
			}
			line := formatFn(rec)
			if filter != "" && !strings.Contains(line, filter) {
				continue
			}
			atomic.AddInt64(&recordCount, 1)
			statsMu.Lock()
			partitionCounts[rec.Partition]++
			if _, seen := partitionFirstOffset[rec.Partition]; !seen {
				partitionFirstOffset[rec.Partition] = rec.Offset
			}
			partitionLastOffset[rec.Partition] = rec.Offset
			statsMu.Unlock()
			app.QueueUpdateDraw(func() {
				_, _ = fmt.Fprintf(view, "%s\n", line)
				view.ScrollToEnd()
			})
		}
	}()

	// Drain error channel independently so errors don't block the consumer.
	go func() {
		for err := range errs {
			msg := err.Error()
			statsMu.Lock()
			consumeErrors = append(consumeErrors, msg)
			statsMu.Unlock()
			app.QueueUpdateDraw(func() {
				_, _ = fmt.Fprintf(view, "[red]error: %s[-]\n", msg)
			})
		}
	}()
}

// toJSONValue embeds s as a raw JSON value if it is already valid JSON,
// otherwise wraps it as a JSON-encoded string. Empty input becomes null.
func toJSONValue(s string) string {
	if s == "" {
		return "null"
	}
	if json.Valid([]byte(s)) {
		return s
	}
	quoted, _ := json.Marshal(s)
	return string(quoted)
}

// defaultConsumeFormat is the -f string the defaults render records with: one JSON object
// per record, the same shape as the kcat templates in config.yaml.
//
// It is single-quoted so that tokenize keeps the double quotes inside it. %k, %T and %h are
// quoted there because -f substitutes them raw, as plain text; only %s is expected to carry
// JSON of its own. A key or header that is not a bare string therefore breaks the JSON shape
// — unlike the no -f path, where formatConsumeRecord escapes whatever it is handed.
const defaultConsumeFormat = `-f '{"Key":"%k","Value":%s,"Timestamp":"%T",` +
	`"Partition":%p,"Offset":%o,"Headers":"%h","Size":%S}'`

// defaultConsumeParams returns the parameters a topic is consumed with when nothing else
// is known about it: tail the last 100 records per partition, decoded through the selected
// schema registry when there is one, rendered with defaultConsumeFormat.
func (app *App) defaultConsumeParams() string {
	if app.Selected.SchemaRegistry != nil {
		// Key and value are spelled out rather than written as the equivalent bare
		// -d avro: a topic with a string key is then one deleted flag away, not a
		// rewrite. -r names the registry they are decoded through, and a payload that
		// is not Avro falls back to raw display.
		return "-o 100 -d key=avro -d value=avro -r " +
			app.Selected.SchemaRegistry.Name + " " + defaultConsumeFormat
	}
	// No registry to decode through, so no -d: avro without -r is refused by prepareConsume.
	return "-o 100 " + defaultConsumeFormat
}

// preparedConsume is a validated consume request, ready to be handed to StartConsuming.
type preparedConsume struct {
	params   consumer.Params
	formatFn func(consumer.Record) string
	filter   string
}

// prepareConsume parses raw kcat-style parameters for topicName and resolves the schema
// registry client and the output format. ok is false when the request is unusable; a status
// message describing why has already been sent in that case.
func (app *App) prepareConsume(topicName, raw string) (preparedConsume, bool) {
	spec, err := consumer.ParseConsumeArgs(raw)
	if err != nil {
		SendStatusError(fmt.Sprintf("[red]%s", err.Error()))
		return preparedConsume{}, false
	}

	kafkaConf := make(kafka.ConfigMap)
	for k, v := range app.Selected.Cluster.Properties {
		_ = kafkaConf.SetKey(k, v)
	}
	params := consumer.Params{
		KafkaConf:   &kafkaConf,
		Topic:       topicName,
		From:        spec.From,
		To:          spec.To,
		ExitOnEnd:   spec.ExitOnEnd,
		Partitions:  spec.Partitions,
		MaxCount:    spec.Count,
		KeySerdes:   spec.KeySerdes,
		ValueSerdes: spec.ValueSerdes,
	}

	// Resolve schema registry by name if avro serdes is requested.
	if spec.KeySerdes.Kind == consumer.SerdesAvro || spec.ValueSerdes.Kind == consumer.SerdesAvro {
		if spec.SRName == "" {
			SendStatusError("[red]-d avro requires -r <sr-name>")
			return preparedConsume{}, false
		}
		srConfig, ok := app.SchemaRegistries[spec.SRName]
		if !ok {
			SendStatusError(
				fmt.Sprintf("[red]schema registry %q not configured", spec.SRName),
			)
			return preparedConsume{}, false
		}
		srClient, ok := app.SchemaRegistryClients[spec.SRName]
		if !ok {
			var clientErr error
			srClient, clientErr = schemaregistry.NewSchemaRegistryClient(srConfig)
			if clientErr != nil {
				SendStatusError(
					fmt.Sprintf("[red]schema registry client: %s", clientErr),
				)
				return preparedConsume{}, false
			}
			app.SchemaRegistryClients[spec.SRName] = srClient
		}
		params.SRClient = srClient
	}

	formatFn := formatConsumeRecord
	if spec.FormatStr != "" {
		fs := spec.FormatStr
		formatFn = func(r consumer.Record) string {
			return consumer.ApplyFormat(r, fs, topicName)
		}
	}

	return preparedConsume{params: params, formatFn: formatFn, filter: spec.Filter}, true
}

// rememberConsumeParams records raw as the parameters last used on topicName. Persisting
// them is best-effort: failing to write the history file must not disturb the consume that
// just started.
func (app *App) rememberConsumeParams(topicName, raw string) {
	app.History.AddConsume(app.Selected.Cluster.Name, topicName, raw)
	if err := app.History.Save(); err != nil {
		log.Error().Err(err).Msg("failed to save consume history")
	}
}

// ConsumeWithDefaultParams starts consuming topicName straight away with the defaults,
// regardless of what was last used on it. The parameters are not remembered: recording them would
// overwrite the entry the parameters modal prefills with, costing the user the flags they
// tuned by hand for the sake of a default they can always get back by pressing <c>.
func (app *App) ConsumeWithDefaultParams(topicName string) {
	prepared, ok := app.prepareConsume(topicName, app.defaultConsumeParams())
	if !ok {
		return
	}

	app.StartConsuming(topicName, prepared.params, prepared.formatFn, prepared.filter)
}

// consumeParamsModalHeight leaves two lines of parameters visible between the borders.
const consumeParamsModalHeight = 4

// ConsumeModal opens a multiline kcat-style consume params input for topicName, prefilled
// with the parameters last used on it.
// Supported flags: -o beginning|end|<n>|s@<ts>|e@<ts>  -f <format>.
func (app *App) ConsumeModal(topicName string) {
	bgColor := tcell.GetColor(app.Colors.Karat.Background)
	placeholderTextColor := tcell.GetColor(app.Colors.Karat.Placeholder)

	defaultText := app.History.LastConsume(app.Selected.Cluster.Name, topicName)
	if defaultText == "" {
		defaultText = app.defaultConsumeParams()
	}

	input := tview.NewTextArea().
		SetText(defaultText, false).
		SetTextStyle(tcell.StyleDefault.Background(bgColor)).
		SetPlaceholderStyle(tcell.StyleDefault.Foreground(placeholderTextColor).Background(bgColor))

	submit := func() {
		text := strings.TrimSpace(input.GetText())
		if text == "" {
			text = defaultText
		}
		raw := strings.ReplaceAll(text, "\n", " ")

		prepared, ok := app.prepareConsume(topicName, raw)
		if !ok {
			return
		}

		app.HideModalPage(ConsumeParams)
		app.StartConsuming(topicName, prepared.params, prepared.formatFn, prepared.filter)
		app.rememberConsumeParams(topicName, raw)
	}

	input.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if IsCtrlEnter(event) {
			submit()
			return nil
		}
		switch event.Key() {
		case tcell.KeyEsc:
			app.HideModalPage(ConsumeParams)
			return nil
		case tcell.KeyF1:
			app.ConsumeHelpModal()
			app.ShowModalPage(ConsumeHelp)
			return nil
		case tcell.KeyCtrlO:
			app.editConsumeParams(input)
			return nil
		case tcell.KeyCtrlR:
			if app.ConsumeHistoryModal(topicName, input, submit) {
				app.ShowModalPage(ConsumeHistory)
			}
			return nil
		}
		return event
	})

	mainFlex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(input, 0, 1, true)
	mainFlex.SetTitle(" Parameters ")
	mainFlex.SetBorder(true)

	modal := util.NewWideModal(mainFlex, consumeParamsModalHeight)
	app.Layout.PagesRegistry.UI.Pages.AddPage(ConsumeParams, modal, true, false)
}

// ConsumeHistoryModal builds the picker over the parameters remembered for the selected
// cluster, topicName's own entries first. Enter puts the selected parameters into input,
// Ctrl+Enter runs them through submit right away. It reports whether there is anything to
// show — an empty history is reported to the user instead, and no page is registered.
func (app *App) ConsumeHistoryModal(
	topicName string,
	input *tview.TextArea,
	submit func(),
) bool {
	entries := app.History.ConsumeFor(app.Selected.Cluster.Name, topicName)
	if len(entries) == 0 {
		SendStatusNote("no consume history yet")
		return false
	}

	labelColor := tcell.GetColor(app.Colors.Karat.Label.FgColor)

	table := tview.NewTable()
	table.SetSelectable(true, false).SetFixed(1, 0).SetBorderPadding(0, 0, 1, 0)
	table.SetSelectedStyle(
		tcell.StyleDefault.Foreground(
			tcell.GetColor(app.Colors.Karat.Selection.FgColor),
		).Background(
			tcell.GetColor(app.Colors.Karat.Selection.BgColor),
		),
	)
	util.SetTableHeaders(table, labelColor, "Params")

	for i, e := range entries {
		// Params take the whole row; tview clips them at the border and the full string
		// still lands in the text area on select.
		table.SetCell(i+1, 0, tview.NewTableCell(e.Params).SetExpansion(1))
	}
	table.Select(1, 0)

	// selected returns the entry under the cursor; the header row is not selectable.
	selected := func() (config.ConsumeEntry, bool) {
		row, _ := table.GetSelection()
		if row < 1 || row > len(entries) {
			return config.ConsumeEntry{}, false
		}
		return entries[row-1], true
	}

	table.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if IsCtrlEnter(event) {
			if e, ok := selected(); ok {
				input.SetText(e.Params, true)
				app.HideModalPage(ConsumeHistory)
				submit()
			}
			return nil
		}
		switch event.Key() {
		case tcell.KeyEnter:
			if e, ok := selected(); ok {
				input.SetText(e.Params, true)
			}
			app.HideModalPage(ConsumeHistory)
			return nil
		case tcell.KeyEsc:
			app.HideModalPage(ConsumeHistory)
			return nil
		}
		return event
	})

	container := tview.NewFlex().SetDirection(tview.FlexRow).AddItem(table, 0, 1, true)
	container.SetTitle(fmt.Sprintf(" Consume History: %s ", topicName)).SetBorder(true)

	modal := util.NewBottomModal(container)
	app.Layout.PagesRegistry.UI.Pages.AddPage(ConsumeHistory, modal, true, false)

	return true
}

// editConsumeParams hands the current parameters to the editor and writes the result back
// into input. The temp file carries the consume reference as comments below the
// parameters so the flags are at hand while editing; they are dropped on the way back.
func (app *App) editConsumeParams(input *tview.TextArea) {
	buf := input.GetText() + "\n\n" + commentOut(app.consumeReference(false)) + "\n"

	edited, ok := app.OpenInEditor("consume-params-*.conf", []byte(buf))
	if !ok {
		return
	}

	input.SetText(stripComments(string(edited)), true)
}

// commentOut prefixes every line of s with "# ".
func commentOut(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight("# "+line, " ")
	}
	return strings.Join(lines, "\n")
}

// stripComments drops the lines starting with '#' — a parameter line never does, since
// every flag starts with '-' — and trims the surrounding blank lines.
func stripComments(s string) string {
	var kept []string
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "#") {
			continue
		}
		kept = append(kept, line)
	}
	return strings.TrimSpace(strings.Join(kept, "\n"))
}

// consumeReferenceTemplate is the reference of all supported consume flags and format
// specifiers. {k}…{/} marks a token highlighted with the label colour, {d}…{/} a dimmed
// one; consumeReference either expands the markers to colour tags or drops them.
const consumeReferenceTemplate = `{k}Flags{/}
  {k}-o{/}    beginning | earliest | end | latest | <n>
               start n messages back per partition, then keep following
               (default: 100; add -e to stop at the high-water mark)
               with -s: or -s@ it limits how many messages are read instead
  {k}-s:{/}<offset>  start from absolute partition offset
  {k}-s@{/}<ts>      start from timestamp
  {k}-e:{/}<offset>  stop at offset, exclusive (requires -s:; overrides -o <n>)
  {k}-e@{/}<ts>      stop at timestamp, exclusive (requires -s@; overrides -o <n>)
  {k}-e{/}           exit when all partitions reach high-water mark
  {k}-p{/}  <n>      restrict to partition (repeatable)
  {k}-d{/}  <serdes> | key=<serdes> | value=<serdes>
             serdes:  avro  |  pack: [>|<][bBhHiIqQcs]+
             > big-endian (recommended)  < little-endian
             b/B int8/uint8   h/H int16/uint16   i/I int32/uint32
             q/Q int64/uint64  c char  s remaining bytes as string (must be last)
             examples:    -d value=avro   -d key=>i   -d value=>qs
             -d avro decodes key and value; a string key needs -d value=avro
  {k}-r{/}  <sr-name>  schema registry name (required for avro; on its own it
             only names the registry — decoding needs -d)
  {k}-f{/}  <format>   output format string (must be last flag)
  {k}|{/}   <pattern>  show only records whose output contains pattern

{k}Format specifiers for -f{/}
  {k}%k{/} key      {k}%s{/} value      {k}%p{/} partition
  {k}%o{/} offset   {k}%T{/} timestamp  {k}%t{/} topic
  {k}%h{/} headers  {k}%S{/} size (bytes)

{d}Timestamp formats{/}  unix-ms | RFC3339 | 2006-01-02T15:04:05.000`

// consumeReference renders consumeReferenceTemplate, with tview colour tags when
// colored is true and as plain text otherwise.
func (app *App) consumeReference(colored bool) string {
	if !colored {
		return strings.NewReplacer("{k}", "", "{d}", "", "{/}", "").
			Replace(consumeReferenceTemplate)
	}
	return strings.NewReplacer(
		"{k}", "["+app.Colors.Karat.Label.FgColor+"]",
		"{d}", "[grey]",
		"{/}", "[-]",
	).Replace(consumeReferenceTemplate)
}

// ConsumeHelpModal shows a read-only reference of all supported consume flags and format specifiers.
func (app *App) ConsumeHelpModal() {
	view := tview.NewTextView().
		SetDynamicColors(true).
		SetText(app.consumeReference(true)).
		SetScrollable(false)
	view.SetBorder(false).SetBorderPadding(0, 0, 1, 1)

	view.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		if event.Key() == tcell.KeyEsc || event.Key() == tcell.KeyF1 {
			app.HideModalPage(ConsumeHelp)
			return nil
		}

		if IsKey(event, ':') {
			// denied call resource modal on this page
			return nil
		}
		return event
	})

	mainFlex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(view, 0, 1, true)
	mainFlex.SetTitle(" Consume Reference ")
	mainFlex.SetBorder(true)

	app.Layout.PagesRegistry.UI.Pages.AddPage(ConsumeHelp, mainFlex, true, false)
}

// ConsumeStatsModal builds and registers the consume-stats modal page.
// It shows per-partition offset ranges and record counts, plus any errors.
// The modal is shown by the F2 handler in StartConsuming.
func (app *App) ConsumeStatsModal(
	topicName string,
	counts, first, last map[int32]int64,
	errors []string,
) {
	labelColor := app.Colors.Karat.Label.FgColor

	parts := make([]int32, 0, len(counts))
	for p := range counts {
		parts = append(parts, p)
	}
	sort.Slice(parts, func(i, j int) bool { return parts[i] < parts[j] })

	// Render table through tabwriter first (no color tags — they would skew column widths).
	// Then colorize the header line by post-processing the flushed output.
	var tw strings.Builder
	w := tabwriter.NewWriter(&tw, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "Partition\tOffsets\tRecords")
	for _, p := range parts {
		_, _ = fmt.Fprintf(w, "%d\t[%d:%d]\t%d\n", p, first[p], last[p], counts[p])
	}
	_ = w.Flush()

	var sb strings.Builder
	lines := strings.SplitN(tw.String(), "\n", 2)
	fmt.Fprintf(&sb, "[%s]%s[-]\n", labelColor, lines[0])
	if len(lines) > 1 {
		sb.WriteString(lines[1])
	}

	if len(errors) > 0 {
		sb.WriteString("\n[red]Errors[-]\n")
		for _, e := range errors {
			fmt.Fprintf(&sb, "  %s\n", e)
		}
	}

	view := tview.NewTextView().
		SetDynamicColors(true).
		SetText(sb.String()).
		SetScrollable(true)
	view.SetBorder(false).SetBorderPadding(0, 0, 1, 1)

	view.SetInputCapture(func(event *tcell.EventKey) *tcell.EventKey {
		switch event.Key() {
		case tcell.KeyEsc, tcell.KeyF2:
			app.HideModalPage(ConsumeStats)
			return nil
		}
		return event
	})

	mainFlex := tview.NewFlex().SetDirection(tview.FlexRow).
		AddItem(view, 0, 1, true)
	mainFlex.SetTitle(fmt.Sprintf(" Consume Stats: %s ", topicName))
	mainFlex.SetBorder(true)

	modal := util.NewModal(mainFlex)
	app.Layout.PagesRegistry.UI.Pages.AddPage(ConsumeStats, modal, true, false)
}

// formatConsumeRecord renders a record as a single JSON line matching:
// {"Key":"%k","Value":%s,"Timestamp":%T,"Partition":%p,"Offset":%o,"Headers":"%h","Size":%S}
func formatConsumeRecord(r consumer.Record) string {
	headersJSON, _ := json.Marshal(strings.Join(r.Headers, ","))

	return fmt.Sprintf(
		`{"Key":%s,"Value":%s,"Timestamp":%d,"Partition":%d,"Offset":%d,"Headers":%s,"Size":%d}`,
		toJSONValue(r.Key),
		toJSONValue(r.Value),
		r.Timestamp.UnixMilli(),
		r.Partition,
		r.Offset,
		string(headersJSON),
		r.Size,
	)
}
