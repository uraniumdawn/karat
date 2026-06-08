// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package franz

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
)

// TopicLogDirSummary provides the actual on-disk size of a topic, aggregated
// across all replicas of all partitions.
type TopicLogDirSummary struct {
	TotalSizeBytes int64

	// Hint is set if one or more replicas did not report their log dir size,
	// meaning TotalSizeBytes may be an undercount.
	Hint string
}

// TopicLogDirSize returns the actual on-disk size of the given topic by issuing a
// DescribeLogDirs request to every broker hosting one of its partitions.
func (c *Client) TopicLogDirSize(ctx context.Context, topic string) (TopicLogDirSummary, error) {
	metadata, err := c.Admin.Metadata(ctx, topic)
	if err != nil {
		return TopicLogDirSummary{}, fmt.Errorf("failed to retrieve metadata: %w", err)
	}

	detail, ok := metadata.Topics[topic]
	if !ok {
		return TopicLogDirSummary{}, fmt.Errorf("topic %q not found", topic)
	}
	if detail.Err != nil {
		return TopicLogDirSummary{}, fmt.Errorf("failed to describe topic %q: %w", topic, detail.Err)
	}

	var topicSet kadm.TopicsSet
	for partition := range detail.Partitions {
		topicSet.Add(topic, partition)
	}

	logDirs, err := c.Admin.DescribeAllLogDirs(ctx, topicSet)
	if err != nil {
		return TopicLogDirSummary{}, fmt.Errorf("failed to describe log dirs: %w", err)
	}

	var totalSize int64
	reportingReplicas := 0
	logDirs.Each(func(dir kadm.DescribedLogDir) {
		if dir.Err != nil {
			return
		}
		dir.Topics.Each(func(p kadm.DescribedLogDirPartition) {
			if p.Topic != topic {
				return
			}
			totalSize += p.Size
			reportingReplicas++
		})
	})

	expectedReplicas := 0
	for _, p := range detail.Partitions {
		expectedReplicas += len(p.Replicas)
	}

	hint := ""
	if reportingReplicas < expectedReplicas {
		hint = fmt.Sprintf(
			"size may not be accurate: log dir information from %d of %d replicas is missing",
			expectedReplicas-reportingReplicas, expectedReplicas,
		)
	}

	return TopicLogDirSummary{TotalSizeBytes: totalSize, Hint: hint}, nil
}
