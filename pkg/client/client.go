// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

// Package client provides a wrapper around the Kafka AdminClient with additional
// functionality for managing Kafka clusters, topics, consumer groups, and configurations.
package client

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/rs/zerolog/log"
	"golang.org/x/exp/maps"

	"github.com/uraniumdawn/karat/pkg/config"
)

// ClusterResult contains the cluster description result with cluster name.
type ClusterResult struct {
	Name string
	kafka.DescribeClusterResult
}

// ResourceResult contains configuration resource results.
type ResourceResult struct {
	Results []kafka.ConfigResourceResult
}

// TopicsResult contains a map of topic names to their metadata.
type TopicsResult struct {
	Result map[string]*kafka.TopicMetadata
}

// TopicResult contains detailed topic information including offsets and ACLs.
type TopicResult struct {
	Name string
	kafka.DescribeTopicsResult
	kafka.DescribeACLsResult
	Config          []kafka.ConfigResourceResult
	startOffsets    map[int32]kafka.Offset
	endOffsets      map[int32]kafka.Offset
	hourAgoOffsets  map[int32]kafka.Offset
	actualSizeBytes *int64
	sizeHint        string
	mx              sync.RWMutex
}

// SetActualSize sets the topic's actual on-disk size in bytes, as reported by
// DescribeLogDirs. When set, String() reports this instead of the heuristic estimate.
// hint may be non-empty to flag a partial/inaccurate result (e.g. missing replicas).
func (r *TopicResult) SetActualSize(bytes int64, hint string) {
	r.mx.Lock()
	defer r.mx.Unlock()
	r.actualSizeBytes = &bytes
	r.sizeHint = hint
}

// GetActualSize returns the topic's actual on-disk size in bytes and any hint set by
// SetActualSize. ok is false if the actual size has not been set.
func (r *TopicResult) GetActualSize() (bytes int64, hint string, ok bool) {
	r.mx.RLock()
	defer r.mx.RUnlock()
	if r.actualSizeBytes == nil {
		return 0, "", false
	}
	return *r.actualSizeBytes, r.sizeHint, true
}

// ConsumerGroupsResult contains the list of consumer groups.
type ConsumerGroupsResult struct {
	kafka.ListConsumerGroupsResult
}

// DescribeConsumerGroupResult contains consumer group details including lag information.
type DescribeConsumerGroupResult struct {
	kafka.DescribeConsumerGroupsResult
	currentOffsets map[TopicPartition]kafka.Offset
	logEndOffsets  map[TopicPartition]kafka.Offset
	lag            map[TopicPartition]kafka.Offset
	prevLagByTopic map[string]int64
	mx             sync.RWMutex
}

// SetPrevLagByTopic sets the previous lag snapshot used to compute trend arrows in String().
// Pass nil to suppress trend display (e.g. on first load).
func (r *DescribeConsumerGroupResult) SetPrevLagByTopic(m map[string]int64) {
	r.prevLagByTopic = m
}

// TopicPartition represents a topic and partition pair.
type TopicPartition struct {
	Topic     string
	Partition int32
}

// Client wraps the Kafka AdminClient with cluster name context.
type Client struct {
	ClusterName    string
	Timeout        time.Duration
	MaxConcurrency int
	*kafka.AdminClient
}

// NewClient creates a new Client wrapping a confluent-kafka-go AdminClient configured
// from the given cluster config, with the given API call timeout and max concurrency.
func NewClient(config *config.ClusterConfig, timeout time.Duration, maxConcurrency int) (*Client, error) {
	conf := &kafka.ConfigMap{}
	for key, value := range config.Properties {
		_ = conf.SetKey(key, value)
	}

	logChan := make(chan kafka.LogEvent)

	_ = conf.SetKey("go.logs.channel.enable", true)
	_ = conf.SetKey("go.logs.channel", logChan)

	go func() {
		for logEvent := range logChan {
			log.Info().Str("kafka_log", logEvent.Message).Msg("librdkafka")
		}
	}()

	adminClient, err := kafka.NewAdminClient(conf)
	if err != nil {
		log.Error().Err(err).Msg("failed to create Admin client")
		return nil, err
	}

	return &Client{
		ClusterName:    config.Name,
		Timeout:        timeout,
		MaxConcurrency: maxConcurrency,
		AdminClient:    adminClient,
	}, nil
}

// DescribeCluster retrieves cluster description including nodes and authorized operations.
func (client *Client) DescribeCluster(resultChan chan<- *ClusterResult, errorChan chan<- error) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
		defer cancel()

		clusterDesc, err := client.AdminClient.DescribeCluster(
			ctx,
			kafka.SetAdminOptionIncludeAuthorizedOperations(true),
		)
		if err != nil {
			errorChan <- err
			return
		}

		result := &ClusterResult{client.ClusterName, clusterDesc}
		resultChan <- result
	}()
}

// DescribeNode retrieves the broker configuration for the given broker ID.
func (client *Client) DescribeNode(
	brokerID string,
	resultChan chan<- *ResourceResult,
	errorChan chan<- error,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
		defer cancel()

		resourceType, err := kafka.ResourceTypeFromString("broker")
		if err != nil {
			errorChan <- fmt.Errorf("failed to parse resource type: %w", err)
			return
		}

		results, err := client.DescribeConfigs(
			ctx,
			[]kafka.ConfigResource{
				{Type: resourceType, Name: brokerID},
			},
			kafka.SetAdminRequestTimeout(client.Timeout),
		)
		if err != nil {
			errorChan <- fmt.Errorf("failed to describe configs: %w", err)
			return
		}

		if len(results) == 0 {
			errorChan <- fmt.Errorf("no results found for brokerID: %s", brokerID)
			return
		}

		result := &ResourceResult{results}
		resultChan <- result
	}()
}

// Topics retrieves metadata for all topics in the cluster.
func (client *Client) Topics(resultChan chan<- *TopicsResult, errorChan chan<- error) {
	go func() {
		metadata, err := client.GetMetadata(nil, true, int(client.Timeout.Milliseconds()))
		if err != nil {
			errorChan <- err
			return
		}

		topics := make(map[string]*kafka.TopicMetadata)
		for key, value := range metadata.Topics {
			v := value
			topics[key] = &v
		}

		result := &TopicsResult{topics}
		resultChan <- result
	}()
}

// ConsumerGroups retrieves a list of all consumer groups in the cluster.
func (client *Client) ConsumerGroups(
	resultChan chan<- *ConsumerGroupsResult,
	errorChan chan<- error,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
		defer cancel()

		groups, err := client.ListConsumerGroups(ctx)
		if err != nil {
			errorChan <- err
			return
		}

		result := &ConsumerGroupsResult{groups}
		resultChan <- result
	}()
}

// TopicMessageCount holds a topic's approximate message count.
type TopicMessageCount struct {
	// Count is the sum over partitions of (highWatermark - logStartOffset). Because it is an
	// offset span, it over-counts compacted topics (compaction leaves offset gaps) and topics
	// with transaction markers; it is exact for plain delete-cleanup, non-transactional topics.
	Count int64

	// Incomplete is true when one or more partitions did not report offsets, making Count a
	// possible undercount.
	Incomplete bool
}

// TopicMessageCounts estimates the message count of every topic in metadata as the sum over its
// partitions of (highWatermark - logStartOffset), using leader offsets (so the count is not
// multiplied by replication factor). It issues two bulk ListOffsets calls — one for the earliest
// offsets and one for the latest — regardless of topic or partition count.
//
// The latest offsets are read with READ_UNCOMMITTED isolation so they are the high watermark (the
// physical log-end offset), matching how Kafka UIs report message count, rather than the
// last-stable-offset. Partitions that report an error are skipped and mark the topic Incomplete.
//
// Note: on large clusters or when a broker is slow this can be expensive — confluent's ListOffsets
// is all-or-nothing, so a single slow leader stalls the whole call until the context deadline.
func (client *Client) TopicMessageCounts(
	ctx context.Context,
	metadata map[string]*kafka.TopicMetadata,
) (map[string]TopicMessageCount, error) {
	earliest := make(map[kafka.TopicPartition]kafka.OffsetSpec)
	latest := make(map[kafka.TopicPartition]kafka.OffsetSpec)
	expected := make(map[string]int, len(metadata))
	for name, meta := range metadata {
		expected[name] = len(meta.Partitions)
		for _, p := range meta.Partitions {
			topic := name
			pID := p.ID
			tp := kafka.TopicPartition{Topic: &topic, Partition: pID}
			earliest[tp] = kafka.EarliestOffsetSpec
			latest[tp] = kafka.LatestOffsetSpec
		}
	}

	result := make(map[string]TopicMessageCount, len(metadata))
	if len(latest) == 0 {
		for name := range metadata {
			result[name] = TopicMessageCount{}
		}
		return result, nil
	}

	// The earliest and latest lookups are independent, so run them concurrently: wall-clock is
	// one ListOffsets round trip instead of two sequential ones.
	var (
		startRes, endRes kafka.ListOffsetsResult
		startErr, endErr error
		wg               sync.WaitGroup
	)
	wg.Add(2)
	go func() {
		defer wg.Done()
		startRes, startErr = client.ListOffsets(ctx, earliest,
			kafka.SetAdminIsolationLevel(kafka.IsolationLevelReadUncommitted))
	}()
	go func() {
		defer wg.Done()
		endRes, endErr = client.ListOffsets(ctx, latest,
			kafka.SetAdminIsolationLevel(kafka.IsolationLevelReadUncommitted))
	}()
	wg.Wait()

	if startErr != nil {
		return nil, fmt.Errorf("failed to list start offsets: %w", startErr)
	}
	if endErr != nil {
		return nil, fmt.Errorf("failed to list end offsets: %w", endErr)
	}

	type span struct {
		low, high     kafka.Offset
		lowOK, highOK bool
	}
	spans := make(map[TopicPartition]*span)
	spanFor := func(tp TopicPartition) *span {
		s := spans[tp]
		if s == nil {
			s = &span{}
			spans[tp] = s
		}
		return s
	}
	for tp, info := range startRes.ResultInfos {
		if info.Error.Code() != kafka.ErrNoError {
			continue
		}
		s := spanFor(TopicPartition{*tp.Topic, tp.Partition})
		s.low, s.lowOK = info.Offset, true
	}
	for tp, info := range endRes.ResultInfos {
		if info.Error.Code() != kafka.ErrNoError {
			continue
		}
		s := spanFor(TopicPartition{*tp.Topic, tp.Partition})
		s.high, s.highOK = info.Offset, true
	}

	reported := make(map[string]int, len(metadata))
	for tp, s := range spans {
		if !s.lowOK || !s.highOK {
			continue
		}
		mc := result[tp.Topic]
		mc.Count += int64(s.high - s.low) // high >= low always holds; no clamp needed
		result[tp.Topic] = mc
		reported[tp.Topic]++
	}

	for name := range metadata {
		mc := result[name]
		if reported[name] < expected[name] {
			mc.Incomplete = true
		}
		result[name] = mc
	}

	return result, nil
}

// ConsumerGroupTotalLags computes the total consumer lag for each group — the sum, over
// every partition the group has committed an offset for, of (logEndOffset - committedOffset),
// counting only positive differences.
//
// The Kafka protocol allows only a single group per ListConsumerGroupOffsets call, so committed
// offsets are fetched with one call per group, fanned out with bounded concurrency. Log-end
// offsets for the union of all partitions are then fetched in a single ListOffsets call, so the
// broker sees N+1 requests total (N = group count), not N per partition. A group whose offsets
// cannot be fetched is omitted from the result (rather than reported as zero lag).
func (client *Client) ConsumerGroupTotalLags(ctx context.Context, groups []string) (map[string]int64, error) {
	concurrency := client.MaxConcurrency
	if concurrency <= 0 {
		concurrency = 8
	}

	type groupOffsets struct {
		group   string
		offsets map[TopicPartition]kafka.Offset
	}

	sem := make(chan struct{}, concurrency)
	resultsCh := make(chan groupOffsets, len(groups))
	var wg sync.WaitGroup

	for _, group := range groups {
		wg.Add(1)
		go func(group string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
				return
			}
			defer func() { <-sem }()

			offsets, err := client.ListConsumerGroupOffsets(
				ctx,
				[]kafka.ConsumerGroupTopicPartitions{{Group: group}},
			)
			if err != nil {
				log.Debug().Err(err).Str("group", group).Msg("failed to list consumer group offsets")
				return
			}

			committed := make(map[TopicPartition]kafka.Offset)
			for _, gtp := range offsets.ConsumerGroupsTopicPartitions {
				for _, tp := range gtp.Partitions {
					if int64(tp.Offset) < 0 {
						continue
					}
					committed[TopicPartition{*tp.Topic, tp.Partition}] = tp.Offset
				}
			}
			resultsCh <- groupOffsets{group: group, offsets: committed}
		}(group)
	}

	wg.Wait()
	close(resultsCh)

	committedByGroup := make(map[string]map[TopicPartition]kafka.Offset, len(groups))
	endTPs := make(map[TopicPartition]struct{})
	for r := range resultsCh {
		committedByGroup[r.group] = r.offsets
		for tp := range r.offsets {
			endTPs[tp] = struct{}{}
		}
	}

	end := make(map[TopicPartition]kafka.Offset, len(endTPs))
	if len(endTPs) > 0 {
		endSpec := make(map[kafka.TopicPartition]kafka.OffsetSpec, len(endTPs))
		for tp := range endTPs {
			topic := tp.Topic
			endSpec[kafka.TopicPartition{Topic: &topic, Partition: tp.Partition}] = kafka.LatestOffsetSpec
		}

		res, err := client.ListOffsets(ctx, endSpec,
			kafka.SetAdminIsolationLevel(kafka.IsolationLevelReadCommitted))
		if err != nil {
			return nil, fmt.Errorf("failed to list end offsets: %w", err)
		}
		for tp, info := range res.ResultInfos {
			end[TopicPartition{*tp.Topic, tp.Partition}] = info.Offset
		}
	}

	return computeGroupLags(committedByGroup, end), nil
}

// computeGroupLags sums, per group, the positive difference between each partition's log-end
// offset and the group's committed offset. Partitions with no known end offset are skipped,
// and negative differences (committed ahead of end) are clamped to zero.
func computeGroupLags(
	committedByGroup map[string]map[TopicPartition]kafka.Offset,
	end map[TopicPartition]kafka.Offset,
) map[string]int64 {
	lags := make(map[string]int64, len(committedByGroup))
	for group, offsets := range committedByGroup {
		var total int64
		for tp, current := range offsets {
			if endOffset, ok := end[tp]; ok {
				if d := int64(endOffset - current); d > 0 {
					total += d
				}
			}
		}
		lags[group] = total
	}
	return lags
}

// CreateTopic creates a new topic with the given name, partition count, replication
// factor, and topic-level config overrides.
func (client *Client) CreateTopic(
	name string,
	numPartitions int,
	replicationFactor int,
	config map[string]string,
	resultChan chan<- bool,
	errorChan chan<- error,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
		defer cancel()

		topicSpec := kafka.TopicSpecification{
			Topic:             name,
			NumPartitions:     numPartitions,
			ReplicationFactor: replicationFactor,
			Config:            config,
		}

		results, err := client.CreateTopics(
			ctx,
			[]kafka.TopicSpecification{topicSpec},
			kafka.SetAdminRequestTimeout(client.Timeout),
		)
		if err != nil {
			errorChan <- err
			return
		}

		for _, result := range results {
			if result.Error.Code() != kafka.ErrNoError {
				errorChan <- fmt.Errorf("failed to create topic '%s': %s", name, result.Error.String())
				return
			}
		}

		resultChan <- true
	}()
}

// DeleteTopic deletes the topic with the given name.
func (client *Client) DeleteTopic(
	name string,
	resultChan chan<- bool,
	errorChan chan<- error,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
		defer cancel()

		results, err := client.DeleteTopics(
			ctx,
			[]string{name},
			kafka.SetAdminRequestTimeout(client.Timeout),
		)
		if err != nil {
			errorChan <- err
			return
		}

		for _, result := range results {
			if result.Error.Code() != kafka.ErrNoError {
				errorChan <- fmt.Errorf("failed to delete topic '%s': %s", name, result.Error.String())
				return
			}
		}

		resultChan <- true
	}()
}

// recreateDeletionTimeout bounds how long RecreateTopic waits for a deleted topic to
// disappear from cluster metadata before re-creating it. recreateDeletionPollWait is the
// interval between metadata polls, and recreateCreateAttempts is how many times create is
// retried while the deleted name is still reserved on the broker.
const (
	recreateDeletionTimeout  = 60 * time.Second
	recreateDeletionPollWait = 500 * time.Millisecond
	recreateCreateAttempts   = 5
)

// RecreateTopic deletes the named topic and re-creates it with the same partition count,
// replication factor, and config. Because Kafka deletion is asynchronous, it waits for the
// topic to disappear from cluster metadata before re-creating, so the create does not fail
// with TOPIC_ALREADY_EXISTS. Sending on resultChan/errorChan follows the same one-shot
// convention as CreateTopic and DeleteTopic.
func (client *Client) RecreateTopic(
	name string,
	numPartitions int,
	replicationFactor int,
	config map[string]string,
	resultChan chan<- bool,
	errorChan chan<- error,
) {
	go func() {
		if err := client.deleteTopicSync(name); err != nil {
			errorChan <- err
			return
		}

		if err := client.waitForTopicAbsence(name); err != nil {
			errorChan <- err
			return
		}

		if err := client.createTopicWithRetry(name, numPartitions, replicationFactor, config); err != nil {
			errorChan <- err
			return
		}

		resultChan <- true
	}()
}

// deleteTopicSync deletes the named topic and blocks until the broker acknowledges the
// request, returning the first per-topic error if any.
func (client *Client) deleteTopicSync(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
	defer cancel()

	results, err := client.DeleteTopics(ctx, []string{name}, kafka.SetAdminRequestTimeout(client.Timeout))
	if err != nil {
		return err
	}

	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError {
			return fmt.Errorf("failed to delete topic '%s': %s", name, result.Error.String())
		}
	}

	return nil
}

// waitForTopicAbsence polls cluster metadata until the named topic is no longer present,
// or until recreateDeletionTimeout elapses.
func (client *Client) waitForTopicAbsence(name string) error {
	deadline := time.Now().Add(recreateDeletionTimeout)
	for {
		metadata, err := client.GetMetadata(nil, true, int(client.Timeout.Milliseconds()))
		if err != nil {
			return fmt.Errorf(
				"failed to fetch metadata while waiting for topic '%s' deletion: %w",
				name,
				err,
			)
		}

		if _, exists := metadata.Topics[name]; !exists {
			return nil
		}

		if time.Now().After(deadline) {
			return fmt.Errorf("timed out waiting for topic '%s' to be deleted", name)
		}

		time.Sleep(recreateDeletionPollWait)
	}
}

// createTopicWithRetry creates the topic, retrying briefly while the broker still reports
// the (just-deleted) name as existing, which can happen when metadata lags the deletion.
func (client *Client) createTopicWithRetry(
	name string,
	numPartitions int,
	replicationFactor int,
	config map[string]string,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
	defer cancel()

	topicSpec := kafka.TopicSpecification{
		Topic:             name,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
		Config:            config,
	}

	for attempt := 1; ; attempt++ {
		results, err := client.CreateTopics(
			ctx,
			[]kafka.TopicSpecification{topicSpec},
			kafka.SetAdminRequestTimeout(client.Timeout),
		)
		if err != nil {
			return err
		}
		if len(results) == 0 {
			return fmt.Errorf("failed to recreate topic '%s': empty create result", name)
		}

		result := results[0]
		code := result.Error.Code()
		if code == kafka.ErrNoError {
			return nil
		}
		if code == kafka.ErrTopicAlreadyExists && attempt < recreateCreateAttempts {
			time.Sleep(recreateDeletionPollWait)
			continue
		}

		return fmt.Errorf("failed to recreate topic '%s': %s", name, result.Error.String())
	}
}

// DeleteConsumerGroup deletes a consumer group.
func (client *Client) DeleteConsumerGroup(
	group string,
	resultChan chan<- bool,
	errorChan chan<- error,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
		defer cancel()

		result, err := client.DeleteConsumerGroups(
			ctx,
			[]string{group},
			kafka.SetAdminRequestTimeout(client.Timeout),
		)
		if err != nil {
			errorChan <- fmt.Errorf("failed to delete consumer group: %w", err)
			return
		}

		for _, r := range result.ConsumerGroupResults {
			if r.Error.Code() != kafka.ErrNoError {
				errorChan <- fmt.Errorf("failed to delete consumer group '%s': %s", group, r.Error)
				return
			}
		}

		resultChan <- true
	}()
}

// UpdateTopicConfig incrementally applies the given config overrides to the named topic.
// Keys listed in remove are reset to the cluster default. Both happen in a single
// IncrementalAlterConfigs call, so the topic never sits in a half-applied state.
func (client *Client) UpdateTopicConfig(
	name string,
	config map[string]string,
	remove []string,
	resultChan chan<- bool,
	errorChan chan<- error,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
		defer cancel()

		resourceType, err := kafka.ResourceTypeFromString("topic")
		if err != nil {
			errorChan <- fmt.Errorf("failed to parse resource type: %w", err)
			return
		}

		// Build config entries for AlterConfigs
		var configEntries []kafka.ConfigEntry
		for key, value := range config {
			configEntries = append(configEntries, kafka.ConfigEntry{
				Name:                 key,
				Value:                value,
				IncrementalOperation: kafka.AlterConfigOpTypeSet,
			})
		}
		for _, key := range remove {
			configEntries = append(configEntries, kafka.ConfigEntry{
				Name:                 key,
				IncrementalOperation: kafka.AlterConfigOpTypeDelete,
			})
		}

		configResource := kafka.ConfigResource{
			Type:   resourceType,
			Name:   name,
			Config: configEntries,
		}

		results, err := client.IncrementalAlterConfigs(
			ctx,
			[]kafka.ConfigResource{configResource},
			kafka.SetAdminRequestTimeout(client.Timeout),
		)
		if err != nil {
			errorChan <- err
			return
		}

		for _, result := range results {
			if result.Error.Code() != kafka.ErrNoError {
				errorChan <- fmt.Errorf("failed to update topic config '%s': %s", name, result.Error.String())
				return
			}
		}

		resultChan <- true
	}()
}

// DescribeConsumerGroup retrieves a consumer group's description along with its
// current offsets, log-end offsets, and per-partition lag.
func (client *Client) DescribeConsumerGroup(
	group string,
	resultChan chan<- *DescribeConsumerGroupResult,
	errorChan chan<- error,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
		defer cancel()

		result := &DescribeConsumerGroupResult{}
		client.CurrentOffsets(ctx, group, errorChan, result)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			client.LogEndOffsets(ctx, maps.Keys(result.currentOffsets), errorChan, result)
		}()

		go func() {
			defer wg.Done()
			groups, err := client.DescribeConsumerGroups(ctx, []string{group})
			if err != nil {
				errorChan <- err
				return
			}
			result.DescribeConsumerGroupsResult = groups
		}()

		wg.Wait()
		result.SetLag(result.currentOffsets, result.logEndOffsets)
		resultChan <- result
	}()
}

// LogEndOffsets fetches the log-end offsets for the given topic partitions and stores
// them on result.
func (client *Client) LogEndOffsets(
	ctx context.Context,
	tps []TopicPartition,
	errorChan chan<- error,
	result *DescribeConsumerGroupResult,
) {
	endOffsets := make(map[kafka.TopicPartition]kafka.OffsetSpec)
	for _, tp := range tps {
		endOffsets[kafka.TopicPartition{
			Topic:     &tp.Topic,
			Partition: tp.Partition,
		}] = kafka.LatestOffsetSpec
	}

	end, err := client.ListOffsets(ctx, endOffsets,
		kafka.SetAdminIsolationLevel(kafka.IsolationLevelReadCommitted))
	if err != nil {
		errorChan <- err
		return
	}
	r := make(map[TopicPartition]kafka.Offset)
	for tp, info := range end.ResultInfos {
		r[TopicPartition{*tp.Topic, tp.Partition}] = info.Offset
	}
	result.SetEndOffsets(r)
}

// CurrentOffsets retrieves the current committed offsets for a consumer group.
func (client *Client) CurrentOffsets(
	ctx context.Context,
	group string,
	errorChan chan<- error,
	result *DescribeConsumerGroupResult,
) {
	currentOffsets := kafka.ConsumerGroupTopicPartitions{
		Group: group,
	}
	offsets, err := client.ListConsumerGroupOffsets(
		ctx,
		[]kafka.ConsumerGroupTopicPartitions{currentOffsets},
	)
	if err != nil {
		errorChan <- err
		return
	}
	r := make(map[TopicPartition]kafka.Offset)
	for _, tps := range offsets.ConsumerGroupsTopicPartitions {
		for _, tp := range tps.Partitions {
			r[TopicPartition{*tp.Topic, tp.Partition}] = tp.Offset
		}
	}
	result.SetCurrentOffsets(r)
}

// SetLag computes per-partition consumer lag as the difference between end and
// current offsets and stores the result.
func (r *DescribeConsumerGroupResult) SetLag(
	current map[TopicPartition]kafka.Offset,
	end map[TopicPartition]kafka.Offset,
) {
	consumerLag := make(map[TopicPartition]kafka.Offset)
	for tp, offsets := range current {
		if endOffset, ok := end[tp]; ok {
			consumerLag[tp] = endOffset - offsets
		}
	}
	r.lag = consumerLag
}

// SetCurrentOffsets stores the consumer group's current committed offsets.
func (r *DescribeConsumerGroupResult) SetCurrentOffsets(o map[TopicPartition]kafka.Offset) {
	r.mx.Lock()
	defer r.mx.Unlock()
	r.currentOffsets = o
}

// SetEndOffsets stores the log-end offsets for the consumer group's partitions.
func (r *DescribeConsumerGroupResult) SetEndOffsets(o map[TopicPartition]kafka.Offset) {
	r.mx.Lock()
	defer r.mx.Unlock()
	r.logEndOffsets = o
}

// CommittedOffset is one partition's committed position, as of the describe call that
// produced it.
type CommittedOffset struct {
	TopicPartition TopicPartition
	Committed      int64
}

// GetCommittedOffsets returns the group's committed offsets, one entry per partition,
// ordered by topic then partition. Partitions the group has not committed on are absent.
func (r *DescribeConsumerGroupResult) GetCommittedOffsets() []CommittedOffset {
	r.mx.RLock()
	defer r.mx.RUnlock()

	offsets := make([]CommittedOffset, 0, len(r.currentOffsets))
	for tp, committed := range r.currentOffsets {
		offsets = append(offsets, CommittedOffset{
			TopicPartition: tp,
			Committed:      int64(committed),
		})
	}

	sort.Slice(offsets, func(i, j int) bool {
		if offsets[i].TopicPartition.Topic != offsets[j].TopicPartition.Topic {
			return offsets[i].TopicPartition.Topic < offsets[j].TopicPartition.Topic
		}
		return offsets[i].TopicPartition.Partition < offsets[j].TopicPartition.Partition
	})

	return offsets
}

// GetTotalLag returns the sum of lag across all partitions.
// Returns the total number of messages the consumer group is behind.
func (r *DescribeConsumerGroupResult) GetTotalLag() int64 {
	var total int64
	for _, lag := range r.lag {
		total += int64(lag)
	}
	return total
}

// GetLagByTopic returns lag aggregated by topic name.
// Returns a map of topic name to total lag across all partitions of that topic.
func (r *DescribeConsumerGroupResult) GetLagByTopic() map[string]int64 {
	lagByTopic := make(map[string]int64)
	for tp, lag := range r.lag {
		lagByTopic[tp.Topic] += int64(lag)
	}
	return lagByTopic
}

// GetPartitionCountByTopic returns the number of partitions per topic.
// Useful for displaying alongside per-topic lag.
func (r *DescribeConsumerGroupResult) GetPartitionCountByTopic() map[string]int {
	partitionCount := make(map[string]int)
	for tp := range r.currentOffsets {
		partitionCount[tp.Topic]++
	}
	return partitionCount
}

// GetTopicNames returns a sorted list of unique topic names in the consumer group.
func (r *DescribeConsumerGroupResult) GetTopicNames() []string {
	topicSet := make(map[string]bool)
	for tp := range r.currentOffsets {
		topicSet[tp.Topic] = true
	}

	topics := make([]string, 0, len(topicSet))
	for topic := range topicSet {
		topics = append(topics, topic)
	}

	sort.Strings(topics)
	return topics
}

// SetStartOffsets stores the per-partition start (earliest) offsets.
func (r *TopicResult) SetStartOffsets(o map[int32]kafka.Offset) {
	r.mx.Lock()
	defer r.mx.Unlock()
	r.startOffsets = o
}

// SetEndOffsets stores the per-partition end (latest) offsets.
func (r *TopicResult) SetEndOffsets(o map[int32]kafka.Offset) {
	r.mx.Lock()
	defer r.mx.Unlock()
	r.endOffsets = o
}

// SetHourAgoOffsets stores the per-partition offsets at one hour ago.
func (r *TopicResult) SetHourAgoOffsets(o map[int32]kafka.Offset) {
	r.mx.Lock()
	defer r.mx.Unlock()
	r.hourAgoOffsets = o
}

// SetTopicResultConfig stores the topic's resource configuration entries.
func (r *TopicResult) SetTopicResultConfig(o []kafka.ConfigResourceResult) {
	r.mx.Lock()
	defer r.mx.Unlock()
	r.Config = o
}

// SetTopicACLsResult stores the topic's ACL description result.
func (r *TopicResult) SetTopicACLsResult(o kafka.DescribeACLsResult) {
	r.mx.Lock()
	defer r.mx.Unlock()
	r.DescribeACLsResult = o
}

// GetTotalMessages returns the total message count across all partitions.
// This is the sum of (end_offset - start_offset) for all partitions.
func (r *TopicResult) GetTotalMessages() int64 {
	var total int64
	for partition := range r.endOffsets {
		end := r.endOffsets[partition]
		start := r.startOffsets[partition]
		total += int64(end - start)
	}
	return total
}

// GetMessagesLastHour returns the total number of messages produced in the last hour across all partitions.
// A partition contributes 0 if its hour-ago offset is negative (no messages produced in the last hour).
func (r *TopicResult) GetMessagesLastHour() int64 {
	var total int64
	for partition, end := range r.endOffsets {
		hourAgo := r.hourAgoOffsets[partition]
		if hourAgo < 0 {
			continue
		}
		total += int64(end - hourAgo)
	}
	return total
}

// TopicPartitionCounts returns the partition count of every topic on the cluster. It is
// what lets an offsets document name a topic the group has never consumed: the partitions
// to seed cannot come from the group's committed offsets, only from cluster metadata.
func (client *Client) TopicPartitionCounts(
	resultChan chan<- map[string]int,
	errorChan chan<- error,
) {
	go func() {
		metadata, err := client.GetMetadata(nil, true, int(client.Timeout.Milliseconds()))
		if err != nil {
			errorChan <- err
			return
		}

		counts := make(map[string]int, len(metadata.Topics))
		for name, topic := range metadata.Topics {
			counts[name] = len(topic.Partitions)
		}

		resultChan <- counts
	}()
}

// OffsetTargetKind identifies how a requested offset is expressed: as an absolute value,
// as one of the partition's watermarks, or as a point in time to look up.
type OffsetTargetKind int

const (
	// OffsetTargetAbsolute is a literal offset, used as given.
	OffsetTargetAbsolute OffsetTargetKind = iota
	// OffsetTargetEarliest resolves to the partition's low watermark.
	OffsetTargetEarliest
	// OffsetTargetLatest resolves to the partition's high watermark.
	OffsetTargetLatest
	// OffsetTargetTimestamp resolves to the first offset at or after TimestampMs.
	OffsetTargetTimestamp
)

// OffsetTarget is one partition's requested new committed offset.
type OffsetTarget struct {
	Kind        OffsetTargetKind
	Offset      int64
	TimestampMs int64
}

// ResolvedOffset is a requested offset after resolution against the cluster, carrying the
// watermarks it was resolved against so the caller can show and range-check it.
type ResolvedOffset struct {
	Target   int64
	Earliest int64
	Latest   int64
}

// InRange reports whether the target is a position the group can actually commit. An
// out-of-range commit is accepted by the broker and then silently overridden by the
// consumer's auto.offset.reset, so it is worth rejecting before it is sent.
func (o ResolvedOffset) InRange() bool {
	return o.Target >= o.Earliest && o.Target <= o.Latest
}

// toKafka converts to the confluent representation, which holds the topic by pointer.
func (tp TopicPartition) toKafka() kafka.TopicPartition {
	topic := tp.Topic
	return kafka.TopicPartition{Topic: &topic, Partition: tp.Partition}
}

// listOffsets resolves one offset spec per partition into absolute offsets.
func (client *Client) listOffsets(
	ctx context.Context,
	request map[kafka.TopicPartition]kafka.OffsetSpec,
) (map[TopicPartition]int64, error) {
	if len(request) == 0 {
		return map[TopicPartition]int64{}, nil
	}

	result, err := client.ListOffsets(ctx, request,
		kafka.SetAdminIsolationLevel(kafka.IsolationLevelReadCommitted))
	if err != nil {
		return nil, err
	}

	offsets := make(map[TopicPartition]int64, len(result.ResultInfos))
	for tp, info := range result.ResultInfos {
		if info.Error.Code() != kafka.ErrNoError {
			return nil, fmt.Errorf(
				"failed to resolve offset for %s[%d]: %s",
				*tp.Topic, tp.Partition, info.Error.String(),
			)
		}
		offsets[TopicPartition{Topic: *tp.Topic, Partition: tp.Partition}] = int64(info.Offset)
	}

	return offsets, nil
}

// ResolveOffsetTargets turns requested offsets into concrete ones, resolving watermarks
// and timestamps against the cluster. Every partition's watermarks are fetched, including
// for absolute targets, so the caller can range-check what the user typed.
func (client *Client) ResolveOffsetTargets(
	targets map[TopicPartition]OffsetTarget,
	resultChan chan<- map[TopicPartition]ResolvedOffset,
	errorChan chan<- error,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
		defer cancel()

		earliestRq := make(map[kafka.TopicPartition]kafka.OffsetSpec, len(targets))
		latestRq := make(map[kafka.TopicPartition]kafka.OffsetSpec, len(targets))
		timestampRq := make(map[kafka.TopicPartition]kafka.OffsetSpec)
		for tp, target := range targets {
			ktp := tp.toKafka()
			earliestRq[ktp] = kafka.EarliestOffsetSpec
			latestRq[ktp] = kafka.LatestOffsetSpec
			if target.Kind == OffsetTargetTimestamp {
				timestampRq[ktp] = kafka.NewOffsetSpecForTimestamp(target.TimestampMs)
			}
		}

		earliest, err := client.listOffsets(ctx, earliestRq)
		if err != nil {
			errorChan <- err
			return
		}
		latest, err := client.listOffsets(ctx, latestRq)
		if err != nil {
			errorChan <- err
			return
		}
		byTimestamp, err := client.listOffsets(ctx, timestampRq)
		if err != nil {
			errorChan <- err
			return
		}

		resolved := make(map[TopicPartition]ResolvedOffset, len(targets))
		for tp, target := range targets {
			offset := ResolvedOffset{Earliest: earliest[tp], Latest: latest[tp]}

			switch target.Kind {
			case OffsetTargetAbsolute:
				offset.Target = target.Offset
			case OffsetTargetEarliest:
				offset.Target = offset.Earliest
			case OffsetTargetLatest:
				offset.Target = offset.Latest
			case OffsetTargetTimestamp:
				// A timestamp past the last record resolves to -1; the end of the log is
				// the only position that satisfies it.
				if ts, ok := byTimestamp[tp]; ok && ts >= 0 {
					offset.Target = ts
				} else {
					offset.Target = offset.Latest
				}
			}

			resolved[tp] = offset
		}

		resultChan <- resolved
	}()
}

// SetConsumerGroupOffsets commits the given absolute offsets for the group in a single
// AlterConsumerGroupOffsets call. The group must have no active members.
func (client *Client) SetConsumerGroupOffsets(
	group string,
	offsets map[TopicPartition]int64,
	resultChan chan<- bool,
	errorChan chan<- error,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
		defer cancel()

		partitions := make([]kafka.TopicPartition, 0, len(offsets))
		for tp, offset := range offsets {
			ktp := tp.toKafka()
			ktp.Offset = kafka.Offset(offset)
			partitions = append(partitions, ktp)
		}

		if len(partitions) == 0 {
			errorChan <- fmt.Errorf("no offsets to set")
			return
		}

		alterResult, err := client.AlterConsumerGroupOffsets(
			ctx,
			[]kafka.ConsumerGroupTopicPartitions{{Group: group, Partitions: partitions}},
			kafka.SetAdminRequestTimeout(client.Timeout),
		)
		if err != nil {
			errorChan <- fmt.Errorf("failed to alter consumer group offsets: %w", err)
			return
		}

		for _, result := range alterResult.ConsumerGroupsTopicPartitions {
			for _, tp := range result.Partitions {
				if tp.Error != nil {
					errorChan <- fmt.Errorf(
						"failed to set offset for %s[%d]: %s",
						*tp.Topic, tp.Partition, tp.Error,
					)
					return
				}
			}
		}

		resultChan <- true
	}()
}

// CopyConsumerGroupOffsets copies committed offsets from sourceGroup to targetGroup,
// implicitly creating targetGroup if it does not exist.
func (client *Client) CopyConsumerGroupOffsets(
	sourceGroup, targetGroup string,
	resultChan chan<- bool,
	errorChan chan<- error,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
		defer cancel()

		listResult, err := client.ListConsumerGroupOffsets(
			ctx,
			[]kafka.ConsumerGroupTopicPartitions{{Group: sourceGroup}},
		)
		if err != nil {
			errorChan <- fmt.Errorf("failed to list consumer group offsets: %w", err)
			return
		}

		var allOffsets []kafka.TopicPartition
		for _, tps := range listResult.ConsumerGroupsTopicPartitions {
			allOffsets = append(allOffsets, tps.Partitions...)
		}

		if len(allOffsets) == 0 {
			errorChan <- fmt.Errorf("no committed offsets found for group %s", sourceGroup)
			return
		}

		alterResult, err := client.AlterConsumerGroupOffsets(
			ctx,
			[]kafka.ConsumerGroupTopicPartitions{
				{Group: targetGroup, Partitions: allOffsets},
			},
			kafka.SetAdminRequestTimeout(client.Timeout),
		)
		if err != nil {
			errorChan <- fmt.Errorf("failed to copy consumer group offsets: %w", err)
			return
		}

		for _, result := range alterResult.ConsumerGroupsTopicPartitions {
			for _, tp := range result.Partitions {
				if tp.Error != nil {
					errorChan <- fmt.Errorf(
						"failed to copy offset for %s[%d]: %s",
						*tp.Topic, tp.Partition, tp.Error,
					)
					return
				}
			}
		}

		resultChan <- true
	}()
}

// ConsumerGroupsByTopic returns consumer groups that have committed offsets for the given topic.
// It fetches topic metadata to discover partitions, then batch-queries all group offsets
// for those partitions and filters groups with at least one valid committed offset.
func (client *Client) ConsumerGroupsByTopic(
	topicName string,
	resultChan chan<- *ConsumerGroupsResult,
	errorChan chan<- error,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
		defer cancel()

		metadata, err := client.GetMetadata(&topicName, false, int(client.Timeout.Milliseconds()))
		if err != nil {
			errorChan <- fmt.Errorf("failed to get topic metadata: %w", err)
			return
		}

		topicMeta, ok := metadata.Topics[topicName]
		if !ok || topicMeta.Error.Code() != kafka.ErrNoError {
			errorChan <- fmt.Errorf("topic %q not found", topicName)
			return
		}

		groups, err := client.ListConsumerGroups(ctx)
		if err != nil {
			errorChan <- fmt.Errorf("failed to list consumer groups: %w", err)
			return
		}

		if len(groups.Valid) == 0 {
			resultChan <- &ConsumerGroupsResult{groups}
			return
		}

		partitions := make([]kafka.TopicPartition, 0, len(topicMeta.Partitions))
		for _, p := range topicMeta.Partitions {
			pID := p.ID
			partitions = append(partitions, kafka.TopicPartition{
				Topic:     &topicName,
				Partition: pID,
			})
		}

		// ListConsumerGroupOffsets only accepts one group per call.
		// Query all groups concurrently with bounded parallelism.
		sem := make(chan struct{}, client.MaxConcurrency)

		type queryResult struct {
			groupID  string
			hasTopic bool
		}

		resultsCh := make(chan queryResult, len(groups.Valid))
		var wg sync.WaitGroup

		for _, g := range groups.Valid {
			groupID := g.GroupID
			wg.Add(1)
			go func() {
				defer wg.Done()
				select {
				case sem <- struct{}{}:
				case <-ctx.Done():
					resultsCh <- queryResult{groupID, false}
					return
				}
				defer func() { <-sem }()

				r, err := client.ListConsumerGroupOffsets(
					ctx,
					[]kafka.ConsumerGroupTopicPartitions{
						{Group: groupID, Partitions: partitions},
					},
				)
				if err != nil {
					resultsCh <- queryResult{groupID, false}
					return
				}
				for _, gtp := range r.ConsumerGroupsTopicPartitions {
					for _, tp := range gtp.Partitions {
						if int64(tp.Offset) >= 0 {
							resultsCh <- queryResult{groupID, true}
							return
						}
					}
				}
				resultsCh <- queryResult{groupID, false}
			}()
		}

		go func() {
			wg.Wait()
			close(resultsCh)
		}()

		groupsWithTopic := make(map[string]bool)
		for r := range resultsCh {
			if r.hasTopic {
				groupsWithTopic[r.groupID] = true
			}
		}

		filteredValid := make([]kafka.ConsumerGroupListing, 0)
		for _, g := range groups.Valid {
			if groupsWithTopic[g.GroupID] {
				filteredValid = append(filteredValid, g)
			}
		}

		resultChan <- &ConsumerGroupsResult{
			kafka.ListConsumerGroupsResult{
				Valid:  filteredValid,
				Errors: groups.Errors,
			},
		}
	}()
}

// IncreasePartitions increases the partition count of a topic to newCount.
// Kafka only supports increasing partition count, not decreasing.
func (client *Client) IncreasePartitions(
	name string,
	newCount int,
	resultChan chan<- bool,
	errorChan chan<- error,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
		defer cancel()

		results, err := client.CreatePartitions(
			ctx,
			[]kafka.PartitionsSpecification{
				{Topic: name, IncreaseTo: newCount},
			},
			kafka.SetAdminRequestTimeout(client.Timeout),
		)
		if err != nil {
			errorChan <- err
			return
		}

		for _, result := range results {
			if result.Error.Code() != kafka.ErrNoError {
				errorChan <- fmt.Errorf("failed to increase partitions for topic '%s': %s", name, result.Error.String())
				return
			}
		}

		resultChan <- true
	}()
}

// DescribeTopic retrieves a topic's description, configuration, ACLs, and start/end/
// hour-ago offsets for all partitions.
func (client *Client) DescribeTopic(
	name string,
	resultChan chan<- *TopicResult,
	errorChan chan<- error,
) {
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
		defer cancel()

		topicResult := &TopicResult{}
		topics := kafka.NewTopicCollectionOfTopicNames(append([]string{}, name))
		desc, err := client.DescribeTopics(
			ctx,
			topics,
			kafka.SetAdminOptionIncludeAuthorizedOperations(true),
		)
		if err != nil {
			errorChan <- err
			return
		}

		topicResult.DescribeTopicsResult = desc

		var wg sync.WaitGroup
		wg.Add(4)
		go func() {
			defer wg.Done()
			tc, errConf := client.DescribeTopicConfig(name)
			topicResult.SetTopicResultConfig(*tc)
			if errConf != nil {
				errorChan <- errConf
				return
			}
		}()

		startOffsetsRq := make(map[kafka.TopicPartition]kafka.OffsetSpec)
		endOffsetsRq := make(map[kafka.TopicPartition]kafka.OffsetSpec)
		hourAgoOffsetsRq := make(map[kafka.TopicPartition]kafka.OffsetSpec)
		hourAgoMs := time.Now().Add(-time.Hour).UnixMilli()
		for _, d := range desc.TopicDescriptions {
			for _, p := range d.Partitions {
				tp := kafka.TopicPartition{Topic: &name, Partition: int32(p.Partition)}
				startOffsetsRq[tp] = kafka.EarliestOffsetSpec
				endOffsetsRq[tp] = kafka.LatestOffsetSpec
				hourAgoOffsetsRq[tp] = kafka.NewOffsetSpecForTimestamp(hourAgoMs)
			}
		}

		toOffsetsByPartition := func(result kafka.ListOffsetsResult) map[int32]kafka.Offset {
			r := make(map[int32]kafka.Offset)
			for tp, info := range result.ResultInfos {
				r[tp.Partition] = info.Offset
			}
			return r
		}

		go func() {
			defer wg.Done()
			st, err := client.ListOffsets(ctx, startOffsetsRq,
				kafka.SetAdminIsolationLevel(kafka.IsolationLevelReadCommitted))
			if err != nil {
				errorChan <- err
				return
			}

			topicResult.SetStartOffsets(toOffsetsByPartition(st))
		}()

		go func() {
			defer wg.Done()
			end, err := client.ListOffsets(ctx, endOffsetsRq,
				kafka.SetAdminIsolationLevel(kafka.IsolationLevelReadCommitted))
			if err != nil {
				errorChan <- err
				return
			}
			topicResult.SetEndOffsets(toOffsetsByPartition(end))
		}()

		go func() {
			defer wg.Done()
			hourAgo, err := client.ListOffsets(ctx, hourAgoOffsetsRq,
				kafka.SetAdminIsolationLevel(kafka.IsolationLevelReadCommitted))
			if err != nil {
				errorChan <- err
				return
			}
			topicResult.SetHourAgoOffsets(toOffsetsByPartition(hourAgo))
		}()

		wg.Wait()
		resultChan <- topicResult
	}()
}

// DescribeTopicConfig retrieves the resource configuration entries for the named topic.
func (client *Client) DescribeTopicConfig(name string) (*[]kafka.ConfigResourceResult, error) {
	ctx, cancel := context.WithTimeout(context.Background(), client.Timeout)
	defer cancel()

	resourceType, err := kafka.ResourceTypeFromString("topic")
	if err != nil {
		return nil, fmt.Errorf("failed to parse resource type: %w", err)
	}

	results, err := client.DescribeConfigs(
		ctx,
		[]kafka.ConfigResource{
			{Type: resourceType, Name: name},
		},
		kafka.SetAdminRequestTimeout(client.Timeout),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to describe configs: %w", err)
	}

	if len(results) == 0 {
		return nil, fmt.Errorf("no results found for topic: %s", name)
	}

	return &results, nil
}
