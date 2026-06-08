// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package franz

import (
	"context"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Transaction summarizes a single transactional ID, as reported by ListTransactions.
type Transaction struct {
	TxnID       string
	State       string
	ProducerID  int64
	Coordinator int32
}

// ListTransactions returns all transactions and their states in the cluster, sorted by
// transactional ID.
func (c *Client) ListTransactions(ctx context.Context) ([]Transaction, error) {
	listed, err := c.Admin.ListTransactions(ctx, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to list transactions: %w", err)
	}

	result := make([]Transaction, 0, len(listed))
	for _, t := range listed.Sorted() {
		result = append(result, Transaction{
			TxnID:       t.TxnID,
			State:       t.State,
			ProducerID:  t.ProducerID,
			Coordinator: t.Coordinator,
		})
	}
	return result, nil
}

// TransactionDetail holds the details of a single transactional ID, as reported by
// DescribeTransactions.
type TransactionDetail struct {
	kadm.DescribedTransaction
}

// DescribeTransaction describes a single transactional ID.
func (c *Client) DescribeTransaction(ctx context.Context, txnID string) (TransactionDetail, error) {
	described, err := c.Admin.DescribeTransactions(ctx, txnID)
	if err != nil {
		return TransactionDetail{}, fmt.Errorf("failed to describe transaction %q: %w", txnID, err)
	}

	detail, err := described.On(txnID, nil)
	if err != nil {
		return TransactionDetail{}, fmt.Errorf("failed to describe transaction %q: %w", txnID, err)
	}
	if detail.Err != nil {
		return TransactionDetail{}, fmt.Errorf("failed to describe transaction %q: %w", txnID, detail.Err)
	}

	return TransactionDetail{detail}, nil
}

// String renders the transaction's details as a summary followed by a table of the
// topic partitions currently part of the transaction.
func (td TransactionDetail) String() string {
	var sb strings.Builder
	w := tabwriter.NewWriter(&sb, 0, 0, 2, ' ', 0)

	_, _ = fmt.Fprintf(w, "Transactional ID:\t%s\n", td.TxnID)
	_, _ = fmt.Fprintf(w, "State:\t%s\n", td.State)
	_, _ = fmt.Fprintf(w, "Coordinator:\t%d\n", td.Coordinator)
	_, _ = fmt.Fprintf(w, "Producer ID:\t%d\n", td.ProducerID)
	_, _ = fmt.Fprintf(w, "Producer Epoch:\t%d\n", td.ProducerEpoch)
	_, _ = fmt.Fprintf(w, "Timeout:\t%d ms\n", td.TimeoutMillis)
	_, _ = fmt.Fprintf(w, "Start Time:\t%s\n", time.UnixMilli(td.StartTimestamp).Format(time.RFC3339))

	_, _ = fmt.Fprintln(w, "\nTopic\tPartition")
	for _, t := range td.Topics.Sorted() {
		for _, p := range t.Partitions {
			_, _ = fmt.Fprintf(w, "%s\t%d\n", t.Topic, p)
		}
	}

	_ = w.Flush()
	return sb.String()
}
