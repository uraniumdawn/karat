// Copyright (c) Sergey Petrovsky
// This source code is licensed under the MIT license found in the
// LICENSE file in the root directory of this source tree.

// Package franz provides a minimal franz-go based Kafka client used for
// functionality not available through confluent-kafka-go's AdminClient
// (e.g. DescribeLogDirs). It is kept isolated from pkg/client on purpose so
// the two Kafka client libraries never mix.
package franz

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/uraniumdawn/karat/pkg/config"
)

// Client wraps a franz-go Kafka client and its admin helper.
type Client struct {
	*kgo.Client
	Admin *kadm.Client
}

// NewClient builds a franz-go Client from the same properties used to configure
// the confluent-kafka-go AdminClient (pkg/client.NewClient). It returns an error
// if the cluster's security configuration cannot be translated to franz-go options.
func NewClient(cfg *config.ClusterConfig, timeout time.Duration) (*Client, error) {
	props := cfg.Properties

	bootstrap := props["bootstrap.servers"]
	if bootstrap == "" {
		return nil, fmt.Errorf("bootstrap.servers is not configured")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(bootstrap, ",")...),
		kgo.ClientID("karat"),
	}

	protocol := strings.ToUpper(props["security.protocol"])
	if strings.HasSuffix(protocol, "SSL") {
		tlsConfig, err := buildTLSConfig(props)
		if err != nil {
			return nil, fmt.Errorf("failed to build TLS config: %w", err)
		}
		opts = append(opts, kgo.DialTLSConfig(tlsConfig))
	}

	if strings.HasPrefix(protocol, "SASL") {
		mechanism, err := buildSASLMechanism(props)
		if err != nil {
			return nil, fmt.Errorf("failed to build SASL mechanism: %w", err)
		}
		opts = append(opts, kgo.SASL(mechanism))
	}

	kafkaClient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create kafka client: %w", err)
	}

	return &Client{
		Client: kafkaClient,
		Admin:  kadm.NewClient(kafkaClient),
	}, nil
}

// buildTLSConfig translates librdkafka ssl.* properties into a tls.Config.
func buildTLSConfig(props map[string]string) (*tls.Config, error) {
	tlsConfig := &tls.Config{}

	if strings.EqualFold(props["ssl.endpoint.identification.algorithm"], "none") {
		tlsConfig.InsecureSkipVerify = true
	}

	if caLocation := props["ssl.ca.location"]; caLocation != "" {
		caCert, err := os.ReadFile(caLocation)
		if err != nil {
			return nil, fmt.Errorf("failed to read ssl.ca.location: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("failed to parse CA certificate from %s", caLocation)
		}
		tlsConfig.RootCAs = pool
	}

	certLocation := props["ssl.certificate.location"]
	keyLocation := props["ssl.key.location"]
	if certLocation != "" && keyLocation != "" {
		cert, err := tls.LoadX509KeyPair(certLocation, keyLocation)
		if err != nil {
			return nil, fmt.Errorf("failed to load client certificate/key: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

// buildSASLMechanism translates librdkafka sasl.* properties into a franz-go SASL mechanism.
// Supported mechanisms: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512.
func buildSASLMechanism(props map[string]string) (sasl.Mechanism, error) {
	mechanism := props["sasl.mechanism"]
	if mechanism == "" {
		mechanism = props["sasl.mechanisms"]
	}
	username := props["sasl.username"]
	password := props["sasl.password"]

	switch strings.ToUpper(mechanism) {
	case "PLAIN":
		return plain.Auth{User: username, Pass: password}.AsMechanism(), nil
	case "SCRAM-SHA-256":
		return scram.Auth{User: username, Pass: password}.AsSha256Mechanism(), nil
	case "SCRAM-SHA-512":
		return scram.Auth{User: username, Pass: password}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("unsupported sasl.mechanism: %q", mechanism)
	}
}
