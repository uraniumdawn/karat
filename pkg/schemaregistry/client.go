// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

// Package schemaregistry provides a client wrapper for Confluent Schema Registry operations.
package schemaregistry

import (
	"sort"

	"github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	"github.com/rs/zerolog/log"

	"github.com/uraniumdawn/karat/pkg/config"
)

// Client wraps the Schema Registry client with cluster name context.
type Client struct {
	ClusterName string
	schemaregistry.Client
}

// SchemaResult contains the schema metadata.
type SchemaResult struct {
	Metadata schemaregistry.SchemaMetadata
}

// NewSchemaRegistryClient creates a new Schema Registry client with the given configuration.
func NewSchemaRegistryClient(config *config.SchemaRegistryConfig) (*Client, error) {
	client, err := schemaregistry.NewClient(schemaregistry.NewConfigWithBasicAuthentication(
		config.SchemaRegistryURL,
		config.SchemaRegistryUsername,
		config.SchemaRegistryPassword))
	if err != nil {
		log.Err(err).Msg("failed to connect to schema registry")
		return nil, err
	}

	return &Client{ClusterName: config.Name, Client: client}, nil
}

// Subjects retrieves all schema subjects from the Schema Registry.
func (client *Client) Subjects(resultChan chan<- []string, errorChan chan<- error) {
	go func() {
		subjects, err := client.GetAllSubjects()
		if err != nil {
			errorChan <- err
			return
		}
		resultChan <- subjects
	}()
}

// VersionsBySubject retrieves all versions for a specific subject.
func (client *Client) VersionsBySubject(
	subject string,
	resultChan chan<- []int,
	errorChan chan<- error,
) {
	go func() {
		versions, err := client.GetAllVersions(subject)
		if err != nil {
			errorChan <- err
			return
		}
		sort.Sort(sort.Reverse(sort.IntSlice(versions)))
		resultChan <- versions
	}()
}

// LatestSchema retrieves the latest schema metadata for a subject.
func (client *Client) LatestSchema(
	subject string,
	resultChan chan<- SchemaResult,
	errorChan chan<- error,
) {
	go func() {
		metadata, err := client.GetLatestSchemaMetadata(subject)
		if err != nil {
			errorChan <- err
			return
		}
		resultChan <- SchemaResult{metadata}
	}()
}

// SubjectConfig retrieves the configuration for a subject.
func (client *Client) SubjectConfig(
	subject string,
	resultChan chan<- schemaregistry.ServerConfig,
	errorChan chan<- error,
) {
	go func() {
		cfg, err := client.GetConfig(subject, true)
		if err != nil {
			errorChan <- err
			return
		}
		resultChan <- cfg
	}()
}

// RegisterSchema registers a schema under a subject, returning the schema ID.
func (client *Client) RegisterSchema(
	subject string,
	schema schemaregistry.SchemaInfo,
	resultChan chan<- int,
	errorChan chan<- error,
) {
	go func() {
		id, err := client.Register(subject, schema, false)
		if err != nil {
			errorChan <- err
			return
		}
		resultChan <- id
	}()
}

// UpdateSubjectConfig sets the configuration for a subject.
func (client *Client) UpdateSubjectConfig(
	subject string,
	config schemaregistry.ServerConfig,
	resultChan chan<- schemaregistry.ServerConfig,
	errorChan chan<- error,
) {
	go func() {
		result, err := client.UpdateConfig(subject, config)
		if err != nil {
			errorChan <- err
			return
		}
		resultChan <- result
	}()
}

// DeleteSubjectVersions soft-deletes all versions of a subject.
func (client *Client) DeleteSubjectVersions(
	subject string,
	resultChan chan<- []int,
	errorChan chan<- error,
) {
	go func() {
		versions, err := client.DeleteSubject(subject, false)
		if err != nil {
			errorChan <- err
			return
		}
		resultChan <- versions
	}()
}

// DeleteVersion soft-deletes a specific version of a subject.
func (client *Client) DeleteVersion(
	subject string,
	version int,
	resultChan chan<- int,
	errorChan chan<- error,
) {
	go func() {
		deleted, err := client.DeleteSubjectVersion(subject, version, false)
		if err != nil {
			errorChan <- err
			return
		}
		resultChan <- deleted
	}()
}

// Schema retrieves a specific schema version for a subject.
func (client *Client) Schema(
	subject string,
	version int,
	resultChan chan<- SchemaResult,
	errorChan chan<- error,
) {
	go func() {
		metadata, err := client.GetSchemaMetadata(subject, version)
		if err != nil {
			errorChan <- err
			return
		}
		resultChan <- SchemaResult{metadata}
	}()
}

// SchemaByID retrieves a schema by its global ID.
func (client *Client) SchemaByID(
	id int,
	resultChan chan<- schemaregistry.SchemaInfo,
	errorChan chan<- error,
) {
	go func() {
		info, err := client.GetBySubjectAndID("", id)
		if err != nil {
			errorChan <- err
			return
		}
		resultChan <- info
	}()
}
