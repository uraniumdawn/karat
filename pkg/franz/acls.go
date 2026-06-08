// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

package franz

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// ACL describes a single access control entry, as reported by DescribeACLs.
type ACL struct {
	Principal  string
	Host       string
	Type       string
	Name       string
	Pattern    string
	Operation  string
	Permission string
}

// ListACLs returns all ACLs in the cluster, sorted by resource type, resource name,
// and principal.
func (c *Client) ListACLs(ctx context.Context) ([]ACL, error) {
	b := kadm.NewACLs().
		AnyResource().
		Allow().AllowHosts().
		Deny().DenyHosts().
		Operations(kadm.OpAny).
		ResourcePatternType(kadm.ACLPatternAny)

	results, err := c.Admin.DescribeACLs(ctx, b)
	if err != nil {
		return nil, fmt.Errorf("failed to list acls: %w", err)
	}

	var acls []ACL
	for _, r := range results {
		if r.Err != nil {
			if errors.Is(r.Err, kerr.SecurityDisabled) {
				return nil, errors.New("ACL authorization is not enabled on this cluster")
			}
			return nil, fmt.Errorf("failed to describe acls for resource type %s: %w", r.Type, r.Err)
		}
		for _, d := range r.Described {
			acls = append(acls, ACL{
				Principal:  d.Principal,
				Host:       d.Host,
				Type:       d.Type.String(),
				Name:       d.Name,
				Pattern:    d.Pattern.String(),
				Operation:  d.Operation.String(),
				Permission: d.Permission.String(),
			})
		}
	}

	sort.Slice(acls, func(i, j int) bool {
		if acls[i].Type != acls[j].Type {
			return acls[i].Type < acls[j].Type
		}
		if acls[i].Name != acls[j].Name {
			return acls[i].Name < acls[j].Name
		}
		return acls[i].Principal < acls[j].Principal
	})
	return acls, nil
}
