// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package franz

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// PartitionProducers holds the active producers for a single partition, as reported by
// DescribeProducers. Err is non-nil if the broker failed to report this partition.
type PartitionProducers struct {
	Partition int32
	Producers []kadm.DescribedProducer
	Err       error
}

// TopicProducers holds the active producers for every partition of a topic, sorted by
// partition.
type TopicProducers struct {
	Partitions []PartitionProducers
}

// DescribeTopicProducers returns, for each partition of the given topic, the
// idempotent/transactional producers whose state the broker is currently tracking, as
// reported by DescribeProducers. Broker-side producer state is retained for
// producer.id.expiration.ms (24h by default on modern brokers) after a producer's last
// write, so an entry here does not necessarily mean the producer is actively writing
// right now. Simple (non-idempotent) producers are never tracked and so never appear. A
// producer with a non-negative CurrentTxnStartOffset has an open transaction starting at
// that offset, which pins the partition's last stable offset (and blocks read_committed
// consumers) until the transaction completes.
func (c *Client) DescribeTopicProducers(ctx context.Context, topic string) (TopicProducers, error) {
	metadata, err := c.Admin.Metadata(ctx, topic)
	if err != nil {
		return TopicProducers{}, fmt.Errorf("failed to retrieve metadata: %w", err)
	}

	detail, ok := metadata.Topics[topic]
	if !ok {
		return TopicProducers{}, fmt.Errorf("topic %q not found", topic)
	}
	if detail.Err != nil {
		return TopicProducers{}, fmt.Errorf("failed to describe topic %q: %w", topic, detail.Err)
	}

	var topicSet kadm.TopicsSet
	for partition := range detail.Partitions {
		topicSet.Add(topic, partition)
	}

	described, err := c.Admin.DescribeProducers(ctx, topicSet)
	if err != nil {
		return TopicProducers{}, fmt.Errorf("failed to describe producers: %w", err)
	}

	dt, ok := described[topic]
	if !ok {
		return TopicProducers{}, nil
	}

	var result TopicProducers
	for _, part := range dt.Partitions.Sorted() {
		pp := PartitionProducers{Partition: part.Partition, Err: part.Err}
		if part.Err == nil {
			pp.Producers = part.ActiveProducers.Sorted()
		}
		result.Partitions = append(result.Partitions, pp)
	}
	return result, nil
}

// String renders the topic's active producers as a table, one row per producer (or a
// placeholder row for partitions with no active producers or a describe error).
func (tp TopicProducers) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 0, 0, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, "Partition\tProducer ID\tEpoch\tLast Sequence\tLast Produce\tOpen Txn Start Offset")
	for _, part := range tp.Partitions {
		if part.Err != nil {
			_, _ = fmt.Fprintf(w, "%d\t<error: %s>\n", part.Partition, part.Err)
			continue
		}
		if len(part.Producers) == 0 {
			_, _ = fmt.Fprintf(w, "%d\t-\t-\t-\t-\t-\n", part.Partition)
			continue
		}
		for _, p := range part.Producers {
			lastProduce := "-"
			if p.LastTimestamp >= 0 {
				lastProduce = time.UnixMilli(p.LastTimestamp).Format(time.RFC3339)
			}
			txnOffset := "-"
			if p.CurrentTxnStartOffset >= 0 {
				txnOffset = strconv.FormatInt(p.CurrentTxnStartOffset, 10)
			}
			_, _ = fmt.Fprintf(w, "%d\t%d\t%d\t%d\t%s\t%s\n",
				part.Partition, p.ProducerID, p.ProducerEpoch, p.LastSequence, lastProduce, txnOffset)
		}
	}
	_ = w.Flush()
	return sb.String()
}
