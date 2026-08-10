// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package ui

import (
	"maps"
	"strings"
	"testing"
	"time"

	"github.com/uraniumdawn/karat/pkg/client"
)

// testCommitted is the group state the document tests render from: two topics, four
// committed partitions.
func testCommitted() []client.CommittedOffset {
	return []client.CommittedOffset{
		{
			TopicPartition: client.TopicPartition{Topic: "orders", Partition: 0},
			Committed:      1284,
		},
		{
			TopicPartition: client.TopicPartition{Topic: "orders", Partition: 1},
			Committed:      1190,
		},
		{
			TopicPartition: client.TopicPartition{Topic: "orders", Partition: 2},
			Committed:      990,
		},
		{
			TopicPartition: client.TopicPartition{Topic: "refunds", Partition: 0},
			Committed:      45,
		},
	}
}

// testPartitionCounts is the cluster metadata the document tests resolve against:
// "orders" and "refunds" are the group's own topics, "payments" is one it has never
// consumed, and "orders" has one partition more than the group has committed on.
func testPartitionCounts() map[string]int {
	return map[string]int{"orders": 4, "refunds": 1, "payments": 2}
}

func tp(topic string, partition int32) client.TopicPartition {
	return client.TopicPartition{Topic: topic, Partition: partition}
}

func TestRenderConsumerGroupOffsetsDocumentRoundTrip(t *testing.T) {
	committed := testCommitted()

	rendered, err := renderConsumerGroupOffsetsDocument("payments-worker", committed)
	if err != nil {
		t.Fatalf("renderConsumerGroupOffsetsDocument() error = %v", err)
	}

	// An untouched document asks for exactly the offsets that are already committed.
	targets, err := parseConsumerGroupOffsetsDocument(
		rendered, "payments-worker", committed, testPartitionCounts(),
	)
	if err != nil {
		t.Fatalf("parseConsumerGroupOffsetsDocument() error = %v\ndocument:\n%s", err, rendered)
	}

	if len(targets) != len(committed) {
		t.Fatalf("got %d targets, want %d\ndocument:\n%s", len(targets), len(committed), rendered)
	}
	for _, offset := range committed {
		target := targets[offset.TopicPartition]
		if target.Kind != client.OffsetTargetAbsolute || target.Offset != offset.Committed {
			t.Errorf("%v = %+v, want absolute %d", offset.TopicPartition, target, offset.Committed)
		}
	}

	// Only the header carries comments; the offsets themselves are bare.
	body, found := strings.CutPrefix(string(rendered), consumerGroupOffsetsHeader)
	if !found {
		t.Fatalf("document does not start with the header:\n%s", rendered)
	}
	if strings.Contains(body, "#") {
		t.Errorf("want no comments in the document body:\n%s", body)
	}
}

func TestRenderConsumerGroupOffsetsDocumentQuotesNumericNames(t *testing.T) {
	rendered, err := renderConsumerGroupOffsetsDocument("123", []client.CommittedOffset{
		{TopicPartition: tp("456", 0), Committed: 1},
	})
	if err != nil {
		t.Fatalf("renderConsumerGroupOffsetsDocument() error = %v", err)
	}

	// A group or topic named "123" must not come back as an integer.
	targets, err := parseConsumerGroupOffsetsDocument(
		rendered,
		"123",
		[]client.CommittedOffset{{TopicPartition: tp("456", 0), Committed: 1}},
		map[string]int{"456": 1},
	)
	if err != nil {
		t.Fatalf("parseConsumerGroupOffsetsDocument() error = %v\ndocument:\n%s", err, rendered)
	}
	if _, ok := targets[tp("456", 0)]; !ok {
		t.Errorf("target for 456[0] missing, got %v\ndocument:\n%s", targets, rendered)
	}
}

// A group with nothing committed is still worth opening: seeding a topic is the only way
// to give it offsets, and the empty "topics: {}" has to say so.
func TestRenderConsumerGroupOffsetsDocumentSeedsAnEmptyGroup(t *testing.T) {
	rendered, err := renderConsumerGroupOffsetsDocument("payments-worker", nil)
	if err != nil {
		t.Fatalf("renderConsumerGroupOffsetsDocument() error = %v", err)
	}

	if !strings.Contains(string(rendered), "no committed offsets") {
		t.Errorf("empty document does not say the group has nothing committed:\n%s", rendered)
	}

	// Naming a topic under "topics:" seeds every partition the cluster reports.
	targets, err := parseConsumerGroupOffsetsDocument(
		[]byte("group: payments-worker\ntopics:\n  orders: earliest\n"),
		"payments-worker",
		nil,
		map[string]int{"orders": 2},
	)
	if err != nil {
		t.Fatalf("parseConsumerGroupOffsetsDocument() error = %v", err)
	}

	want := map[client.TopicPartition]client.OffsetTarget{
		tp("orders", 0): {Kind: client.OffsetTargetEarliest},
		tp("orders", 1): {Kind: client.OffsetTargetEarliest},
	}
	if !maps.Equal(targets, want) {
		t.Errorf("targets = %v, want %v", targets, want)
	}
}

// The per-topic document starts every topic at "none", so saving it untouched must leave
// the group alone.
func TestRenderConsumerGroupTopicOffsetsDocumentStartsAtNone(t *testing.T) {
	committed := testCommitted()
	counts := testPartitionCounts()

	rendered, err := renderConsumerGroupTopicOffsetsDocument("payments-worker", committed)
	if err != nil {
		t.Fatalf("renderConsumerGroupTopicOffsetsDocument() error = %v", err)
	}

	body, found := strings.CutPrefix(string(rendered), consumerGroupTopicOffsetsHeader)
	if !found {
		t.Fatalf("document does not start with the header:\n%s", rendered)
	}
	for _, want := range []string{"orders: none", "refunds: none"} {
		if !strings.Contains(body, want) {
			t.Errorf("document is missing %q:\n%s", want, body)
		}
	}
	// Only the header carries comments; the topics themselves are bare.
	if strings.Contains(body, "#") {
		t.Errorf("want no comments in the document body:\n%s", body)
	}

	targets, err := parseConsumerGroupOffsetsDocument(rendered, "payments-worker", committed, counts)
	if err != nil {
		t.Fatalf("parseConsumerGroupOffsetsDocument() error = %v\ndocument:\n%s", err, rendered)
	}
	if len(targets) != 0 {
		t.Errorf("an untouched per-topic document asks for %v, want nothing", targets)
	}
}

// One value replaced in the per-topic document covers every partition of that topic and
// leaves the others as they were.
func TestRenderConsumerGroupTopicOffsetsDocumentAppliesTopicWide(t *testing.T) {
	committed := testCommitted()
	counts := testPartitionCounts()

	rendered, err := renderConsumerGroupTopicOffsetsDocument("payments-worker", committed)
	if err != nil {
		t.Fatalf("renderConsumerGroupTopicOffsetsDocument() error = %v", err)
	}

	edited := strings.Replace(string(rendered), "orders: none", "orders: earliest", 1)
	targets, err := parseConsumerGroupOffsetsDocument(
		[]byte(edited), "payments-worker", committed, counts,
	)
	if err != nil {
		t.Fatalf("parseConsumerGroupOffsetsDocument() error = %v\ndocument:\n%s", err, edited)
	}

	want := map[client.TopicPartition]client.OffsetTarget{
		tp("orders", 0): {Kind: client.OffsetTargetEarliest},
		tp("orders", 1): {Kind: client.OffsetTargetEarliest},
		tp("orders", 2): {Kind: client.OffsetTargetEarliest},
		tp("orders", 3): {Kind: client.OffsetTargetEarliest},
	}
	if !maps.Equal(targets, want) {
		t.Errorf("targets = %v, want %v", targets, want)
	}
}

func TestParseConsumerGroupOffsetsDocument(t *testing.T) {
	const header = "group: payments-worker\ntopics:\n"

	tests := []struct {
		name     string
		document string
		want     map[client.TopicPartition]client.OffsetTarget
		wantErr  string
	}{
		{
			name:     "per partition absolute offsets",
			document: header + "  orders:\n    0: 900\n    2: 1000\n",
			want: map[client.TopicPartition]client.OffsetTarget{
				tp("orders", 0): {Kind: client.OffsetTargetAbsolute, Offset: 900},
				tp("orders", 2): {Kind: client.OffsetTargetAbsolute, Offset: 1000},
			},
		},
		{
			// "orders" has four partitions on the cluster and the group has committed on
			// three, so naming the topic covers the fourth as well.
			name:     "topic scalar fans out to every cluster partition",
			document: header + "  refunds: earliest\n  orders: latest\n",
			want: map[client.TopicPartition]client.OffsetTarget{
				tp("orders", 0):  {Kind: client.OffsetTargetLatest},
				tp("orders", 1):  {Kind: client.OffsetTargetLatest},
				tp("orders", 2):  {Kind: client.OffsetTargetLatest},
				tp("orders", 3):  {Kind: client.OffsetTargetLatest},
				tp("refunds", 0): {Kind: client.OffsetTargetEarliest},
			},
		},
		{
			name:     "a topic the group never consumed can be seeded",
			document: header + "  payments: earliest\n",
			want: map[client.TopicPartition]client.OffsetTarget{
				tp("payments", 0): {Kind: client.OffsetTargetEarliest},
				tp("payments", 1): {Kind: client.OffsetTargetEarliest},
			},
		},
		{
			name:     "a new topic can be seeded per partition",
			document: header + "  payments:\n    1: 0\n",
			want: map[client.TopicPartition]client.OffsetTarget{
				tp("payments", 1): {Kind: client.OffsetTargetAbsolute, Offset: 0},
			},
		},
		{
			name:     "all applies to every committed topic",
			document: "group: payments-worker\nall: earliest\n",
			want: map[client.TopicPartition]client.OffsetTarget{
				tp("orders", 0):  {Kind: client.OffsetTargetEarliest},
				tp("orders", 1):  {Kind: client.OffsetTargetEarliest},
				tp("orders", 2):  {Kind: client.OffsetTargetEarliest},
				tp("orders", 3):  {Kind: client.OffsetTargetEarliest},
				tp("refunds", 0): {Kind: client.OffsetTargetEarliest},
			},
		},
		{
			// "all" never reaches a topic the group has not consumed; seeding one has to
			// be asked for by name.
			name:     "all does not seed unconsumed topics",
			document: "group: payments-worker\nall: latest\ntopics:\n  payments: earliest\n",
			want: map[client.TopicPartition]client.OffsetTarget{
				tp("orders", 0):   {Kind: client.OffsetTargetLatest},
				tp("orders", 1):   {Kind: client.OffsetTargetLatest},
				tp("orders", 2):   {Kind: client.OffsetTargetLatest},
				tp("orders", 3):   {Kind: client.OffsetTargetLatest},
				tp("refunds", 0):  {Kind: client.OffsetTargetLatest},
				tp("payments", 0): {Kind: client.OffsetTargetEarliest},
				tp("payments", 1): {Kind: client.OffsetTargetEarliest},
			},
		},
		{
			name:     "topics override all",
			document: "group: payments-worker\nall: earliest\ntopics:\n  orders:\n    0: 900\n",
			want: map[client.TopicPartition]client.OffsetTarget{
				tp("orders", 0):  {Kind: client.OffsetTargetAbsolute, Offset: 900},
				tp("orders", 1):  {Kind: client.OffsetTargetEarliest},
				tp("orders", 2):  {Kind: client.OffsetTargetEarliest},
				tp("orders", 3):  {Kind: client.OffsetTargetEarliest},
				tp("refunds", 0): {Kind: client.OffsetTargetEarliest},
			},
		},
		{
			name:     "none excludes a topic from all",
			document: "group: payments-worker\nall: earliest\ntopics:\n  orders: none\n",
			want: map[client.TopicPartition]client.OffsetTarget{
				tp("refunds", 0): {Kind: client.OffsetTargetEarliest},
			},
		},
		{
			name:     "none excludes a single partition",
			document: "group: payments-worker\nall: earliest\ntopics:\n  orders:\n    0: none\n    1: none\n    2: none\n    3: none\n",
			want: map[client.TopicPartition]client.OffsetTarget{
				tp("refunds", 0): {Kind: client.OffsetTargetEarliest},
			},
		},
		{
			name:     "kafka-consumer-groups strategy names are accepted",
			document: header + "  refunds: to-earliest\n  orders: to-latest\n",
			want: map[client.TopicPartition]client.OffsetTarget{
				tp("orders", 0):  {Kind: client.OffsetTargetLatest},
				tp("orders", 1):  {Kind: client.OffsetTargetLatest},
				tp("orders", 2):  {Kind: client.OffsetTargetLatest},
				tp("orders", 3):  {Kind: client.OffsetTargetLatest},
				tp("refunds", 0): {Kind: client.OffsetTargetEarliest},
			},
		},
		{
			name:     "keywords and timestamps mix with offsets",
			document: header + "  orders:\n    0: 900\n    1: latest\n    2: \"@1754006400000\"\n",
			want: map[client.TopicPartition]client.OffsetTarget{
				tp("orders", 0): {Kind: client.OffsetTargetAbsolute, Offset: 900},
				tp("orders", 1): {Kind: client.OffsetTargetLatest},
				tp("orders", 2): {Kind: client.OffsetTargetTimestamp, TimestampMs: 1754006400000},
			},
		},
		{
			name:     "formatted timestamp",
			document: header + "  refunds: \"@2026-08-01T00:00:00.000\"\n",
			want: map[client.TopicPartition]client.OffsetTarget{
				tp("refunds", 0): {
					Kind:        client.OffsetTargetTimestamp,
					TimestampMs: time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC).UnixMilli(),
				},
			},
		},
		{
			name:     "a dropped topic is left untouched",
			document: header + "  orders:\n    0: 900\n",
			want: map[client.TopicPartition]client.OffsetTarget{
				tp("orders", 0): {Kind: client.OffsetTargetAbsolute, Offset: 900},
			},
		},
		{
			name:     "no topics block",
			document: "group: payments-worker\n",
			want:     map[client.TopicPartition]client.OffsetTarget{},
		},
		{
			name:     "group cannot be changed",
			document: "group: other\ntopics:\n  orders: latest\n",
			wantErr:  "group cannot be changed ('payments-worker' -> 'other')",
		},
		{
			name:     "topic that is not on the cluster",
			document: header + "  paymnets: latest\n",
			wantErr:  "topic 'paymnets' does not exist on the cluster",
		},
		{
			name:     "partition beyond the topic",
			document: header + "  orders:\n    7: 900\n",
			wantErr:  "orders has no partition 7",
		},
		{
			name:     "to-offset needs the offset itself",
			document: header + "  orders: to-offset\n",
			wantErr:  "write the offset itself, as in '900'",
		},
		{
			name:     "to-timestamp needs the timestamp itself",
			document: header + "  orders: to-timestamp\n",
			wantErr:  "write the timestamp itself",
		},
		{
			name:     "negative offset",
			document: header + "  orders:\n    0: -5\n",
			wantErr:  "is not an offset",
		},
		{
			name:     "unknown keyword",
			document: header + "  orders: newest\n",
			wantErr:  "is not an offset, 'earliest', 'latest', '@<timestamp>' or 'none'",
		},
		{
			name:     "malformed timestamp",
			document: header + "  orders: \"@not-a-time\"\n",
			wantErr:  "'not-a-time' is not a timestamp",
		},
		{
			name:     "unknown top level key",
			document: "group: payments-worker\nstate: Empty\n",
			wantErr:  "is not a valid key",
		},
		{
			name:     "nested too deep",
			document: header + "  orders:\n    0:\n      a: b\n",
			wantErr:  "expected a single value",
		},
		{
			name:     "empty document",
			document: "\n\n",
			wantErr:  "document is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseConsumerGroupOffsetsDocument(
				[]byte(tt.document),
				"payments-worker",
				testCommitted(),
				testPartitionCounts(),
			)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("error = nil, want one containing %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %q, want it to contain %q", err, tt.wantErr)
				}
				return
			}

			if err != nil {
				t.Fatalf("error = %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("got %d targets %v, want %d %v", len(got), got, len(tt.want), tt.want)
			}
			for partition, want := range tt.want {
				if got[partition] != want {
					t.Errorf("%v = %+v, want %+v", partition, got[partition], want)
				}
			}
		})
	}
}

func TestOffsetChanges(t *testing.T) {
	committed := testCommitted()
	targets := map[client.TopicPartition]client.OffsetTarget{
		tp("orders", 0):  {Kind: client.OffsetTargetAbsolute, Offset: 900},
		tp("orders", 1):  {Kind: client.OffsetTargetAbsolute, Offset: 1190}, // unchanged
		tp("orders", 2):  {Kind: client.OffsetTargetLatest},
		tp("refunds", 0): {Kind: client.OffsetTargetEarliest},
	}
	resolved := map[client.TopicPartition]client.ResolvedOffset{
		tp("orders", 0):  {Target: 900, Earliest: 0, Latest: 1500},
		tp("orders", 1):  {Target: 1190, Earliest: 0, Latest: 1190},
		tp("orders", 2):  {Target: 1400, Earliest: 0, Latest: 1400},
		tp("refunds", 0): {Target: 0, Earliest: 0, Latest: 45},
	}

	changes := offsetChanges(committed, targets, resolved)

	// orders[1] already sits on its target and must not be committed again.
	if len(changes) != 3 {
		t.Fatalf("got %d changes %v, want 3", len(changes), changes)
	}

	want := []offsetChange{
		{TopicPartition: tp("orders", 0), From: 1284, HasFrom: true, To: 900, InRange: true},
		{
			TopicPartition: tp("orders", 2), From: 990, HasFrom: true,
			To: 1400, Note: "latest", InRange: true,
		},
		{
			TopicPartition: tp("refunds", 0), From: 45, HasFrom: true,
			To: 0, Note: "earliest", InRange: true,
		},
	}
	for i, change := range changes {
		if change != want[i] {
			t.Errorf("changes[%d] = %+v, want %+v", i, change, want[i])
		}
	}
}

func TestOffsetChangesSeedsNewPartitions(t *testing.T) {
	committed := testCommitted()
	targets := map[client.TopicPartition]client.OffsetTarget{
		tp("payments", 0): {Kind: client.OffsetTargetEarliest},
		tp("orders", 3):   {Kind: client.OffsetTargetEarliest}, // topic grew past the group
	}
	resolved := map[client.TopicPartition]client.ResolvedOffset{
		tp("payments", 0): {Target: 0, Earliest: 0, Latest: 900},
		tp("orders", 3):   {Target: 12, Earliest: 12, Latest: 400},
	}

	changes := offsetChanges(committed, targets, resolved)
	if len(changes) != 2 {
		t.Fatalf("got %d changes %v, want 2", len(changes), changes)
	}

	// A partition with no prior commit must not claim it is moving from offset 0.
	for _, change := range changes {
		if change.HasFrom {
			t.Errorf("%v HasFrom = true, want false", change.TopicPartition)
		}
		if got := change.fromText(); got != "(none)" {
			t.Errorf("%v fromText() = %q, want \"(none)\"", change.TopicPartition, got)
		}
	}

	rendered := renderOffsetChanges("payments-worker", changes)
	if !strings.Contains(rendered, "payments[0]  (none) -> 0 (earliest)") {
		t.Errorf("renderOffsetChanges() missing the seed line in:\n%s", rendered)
	}
}

func TestOffsetChangesFlagsOutOfRange(t *testing.T) {
	committed := testCommitted()
	targets := map[client.TopicPartition]client.OffsetTarget{
		tp("orders", 0): {Kind: client.OffsetTargetAbsolute, Offset: 12840}, // typo for 1284
		tp("orders", 1): {Kind: client.OffsetTargetAbsolute, Offset: 500},
	}
	resolved := map[client.TopicPartition]client.ResolvedOffset{
		tp("orders", 0): {Target: 12840, Earliest: 0, Latest: 1500},
		tp("orders", 1): {Target: 500, Earliest: 0, Latest: 1190},
	}

	invalid := outOfRangeChanges(offsetChanges(committed, targets, resolved))
	if len(invalid) != 1 || invalid[0].TopicPartition != tp("orders", 0) {
		t.Fatalf("out of range = %v, want orders[0] only", invalid)
	}

	got := outOfRangeMessage(invalid, resolved)
	want := "offset out of range: orders[0] 12840 not in [0, 1500]"
	if got != want {
		t.Errorf("outOfRangeMessage() = %q, want %q", got, want)
	}
}

func TestOutOfRangeMessageTruncates(t *testing.T) {
	var invalid []offsetChange
	resolved := map[client.TopicPartition]client.ResolvedOffset{}
	for partition := int32(0); partition < 5; partition++ {
		partitionKey := tp("orders", partition)
		invalid = append(invalid, offsetChange{TopicPartition: partitionKey, To: 9999})
		resolved[partitionKey] = client.ResolvedOffset{Target: 9999, Earliest: 0, Latest: 10}
	}

	got := outOfRangeMessage(invalid, resolved)
	if !strings.HasSuffix(got, "(and 2 more)") {
		t.Errorf("outOfRangeMessage() = %q, want it to end with the remainder count", got)
	}
	if strings.Count(got, "orders[") != outOfRangeListLimit {
		t.Errorf("outOfRangeMessage() = %q, want %d listed partitions", got, outOfRangeListLimit)
	}
}

func TestResolvedOffsetInRange(t *testing.T) {
	tests := []struct {
		name   string
		offset client.ResolvedOffset
		want   bool
	}{
		{"inside", client.ResolvedOffset{Target: 500, Earliest: 0, Latest: 1000}, true},
		{"at earliest", client.ResolvedOffset{Target: 0, Earliest: 0, Latest: 1000}, true},
		{"at latest", client.ResolvedOffset{Target: 1000, Earliest: 0, Latest: 1000}, true},
		{"below earliest", client.ResolvedOffset{Target: 5, Earliest: 10, Latest: 1000}, false},
		{"above latest", client.ResolvedOffset{Target: 1001, Earliest: 0, Latest: 1000}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.offset.InRange(); got != tt.want {
				t.Errorf("InRange() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRenderOffsetChanges(t *testing.T) {
	changes := []offsetChange{
		{TopicPartition: tp("orders", 0), From: 1284, HasFrom: true, To: 900, InRange: true},
		{
			TopicPartition: tp("orders", 2), From: 990, HasFrom: true,
			To: 1400, Note: "latest", InRange: true,
		},
	}

	got := renderOffsetChanges("payments-worker", changes)

	for _, want := range []string{
		"Consumer group: payments-worker",
		"orders[0]  1284 -> 900",
		"orders[2]  990 -> 1400 (latest)",
		"2 partitions will be committed.",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("renderOffsetChanges() missing %q in:\n%s", want, got)
		}
	}

	single := renderOffsetChanges("payments-worker", changes[:1])
	if !strings.Contains(single, "1 partition will be committed.") {
		t.Errorf("renderOffsetChanges() = %q, want a singular count", single)
	}
}
