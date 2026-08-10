// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package franz

import (
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// described builds one DescribeACLs result carrying a single ACL.
func described(resource, principal string) kadm.DescribeACLsResult {
	return kadm.DescribeACLsResult{
		Type: kmsg.ACLResourceTypeTopic,
		Described: []kadm.DescribedACL{{
			Principal:  principal,
			Host:       "*",
			Type:       kmsg.ACLResourceTypeTopic,
			Name:       resource,
			Pattern:    kadm.ACLPatternLiteral,
			Operation:  kadm.OpRead,
			Permission: kmsg.ACLPermissionTypeAllow,
		}},
	}
}

// A cluster with no authorizer answers every resource type with SECURITY_DISABLED. Reporting
// that as an empty list would read as "this cluster has no ACLs", i.e. nothing to restrict,
// when in fact nothing is enforced at all.
func TestACLsFromResultsReportsADisabledAuthorizer(t *testing.T) {
	results := kadm.DescribeACLsResults{
		{Type: kmsg.ACLResourceTypeTopic, Err: kerr.SecurityDisabled},
		{Type: kmsg.ACLResourceTypeGroup, Err: kerr.SecurityDisabled},
	}

	acls, err := aclsFromResults(results)
	if err == nil {
		t.Fatalf("aclsFromResults() = %v, want an error", acls)
	}
	if !strings.Contains(err.Error(), "not enabled") {
		t.Errorf("error = %q, want it to say authorization is not enabled", err)
	}
	if acls != nil {
		t.Errorf("acls = %v, want none alongside the error", acls)
	}
}

// Any other per-resource failure is reported as itself, naming the resource type.
func TestACLsFromResultsReportsOtherErrors(t *testing.T) {
	results := kadm.DescribeACLsResults{
		{Type: kmsg.ACLResourceTypeTopic, Err: kerr.ClusterAuthorizationFailed},
	}

	_, err := aclsFromResults(results)
	if err == nil {
		t.Fatal("aclsFromResults() accepted a failed result")
	}
	if !strings.Contains(err.Error(), "TOPIC") {
		t.Errorf("error = %q, want it to name the resource type", err)
	}
}

func TestACLsFromResultsSortsRows(t *testing.T) {
	results := kadm.DescribeACLsResults{
		described("orders", "User:bob"),
		described("audit", "User:carol"),
		described("orders", "User:alice"),
	}

	acls, err := aclsFromResults(results)
	if err != nil {
		t.Fatalf("aclsFromResults() error = %v", err)
	}

	want := []struct{ name, principal string }{
		{"audit", "User:carol"},
		{"orders", "User:alice"},
		{"orders", "User:bob"},
	}
	if len(acls) != len(want) {
		t.Fatalf("got %d acls, want %d", len(acls), len(want))
	}
	for i, w := range want {
		if acls[i].Name != w.name || acls[i].Principal != w.principal {
			t.Errorf("row %d = %s/%s, want %s/%s", i, acls[i].Name, acls[i].Principal, w.name, w.principal)
		}
	}
}
