// Command data-generator writes Avro-encoded records to the local Kafka cluster so that karat's
// topic, consumer group and Schema Registry pages have live data to show. Records are framed in
// the Confluent wire format (magic byte, schema id, Avro binary), which is what kcat's
// "-d key=avro -d value=avro" expects.
package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/hamba/avro/v2"
	"github.com/segmentio/kafka-go"
)

// wireFormatMagicByte prefixes every Confluent-framed payload.
const wireFormatMagicByte = 0

var (
	kafkaBroker    = getEnv("KAFKA_BROKER", "localhost:9092")
	schemaRegistry = getEnv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
	schemaDir      = getEnv("SCHEMA_DIR", "/schemas")
	alphaTopic     = getEnv("ALPHA_TOPIC", "stream-alpha")
	betaTopic      = getEnv("BETA_TOPIC", "stream-beta")
	eventRate      = getEnvInt("EVENT_RATE", 50)
	linkRatio      = getEnvFloat("LINK_RATIO", 0.1)

	groups  = generateStringSlice("group-", 10)
	labels  = generateStringSlice("label-", 100)
	kinds   = []string{"one", "two", "three"}
	regions = []string{"north", "south", "east", "west"}
)

// Alpha is the record written to the alpha topic.
type Alpha struct {
	ID        string  `avro:"id"`
	Group     string  `avro:"group"`
	Label     string  `avro:"label"`
	Kind      string  `avro:"kind"`
	Region    string  `avro:"region"`
	CreatedAt int64   `avro:"created_at"`
	Amount    float64 `avro:"amount"`
}

// Beta is the record written to the beta topic; it references an alpha record.
type Beta struct {
	ID        string `avro:"id"`
	AlphaID   string `avro:"alpha_id"`
	Group     string `avro:"group"`
	CreatedAt int64  `avro:"created_at"`
}

// codec encodes values with one registered schema.
type codec struct {
	id     int
	schema avro.Schema
}

// encode returns v as an Avro payload framed in the Confluent wire format.
func (c *codec) encode(v any) ([]byte, error) {
	payload, err := avro.Marshal(c.schema, v)
	if err != nil {
		return nil, fmt.Errorf("marshalling avro: %w", err)
	}

	framed := make([]byte, 5+len(payload))
	framed[0] = wireFormatMagicByte
	binary.BigEndian.PutUint32(framed[1:5], uint32(c.id)) //nolint:gosec // schema ids are small
	copy(framed[5:], payload)

	return framed, nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	alphaKey, err := register(ctx, alphaTopic+"-key")
	if err != nil {
		fatal(err)
	}
	alphaValue, err := register(ctx, alphaTopic+"-value")
	if err != nil {
		fatal(err)
	}
	betaKey, err := register(ctx, betaTopic+"-key")
	if err != nil {
		fatal(err)
	}
	betaValue, err := register(ctx, betaTopic+"-value")
	if err != nil {
		fatal(err)
	}

	alphaWriter := newKafkaWriter(alphaTopic)
	betaWriter := newKafkaWriter(betaTopic)
	defer alphaWriter.Close()
	defer betaWriter.Close()

	ticker := time.NewTicker(time.Second / time.Duration(eventRate))
	defer ticker.Stop()

	fmt.Printf("Producing to %s and %s at %d events/s\n", alphaTopic, betaTopic, eventRate)

	for {
		select {
		case <-ctx.Done():
			fmt.Println("Shutting down.")
			return

		case <-ticker.C:
			alpha := generateAlpha()
			if err := write(ctx, alphaWriter, alphaKey, alphaValue, alpha.ID, alpha); err != nil {
				fmt.Fprintf(os.Stderr, "writing to %s: %v\n", alphaTopic, err)
				continue
			}

			if rand.Float64() >= linkRatio { //nolint:gosec // sample data, not crypto
				continue
			}

			beta := Beta{
				ID:        uuid.New().String(),
				AlphaID:   alpha.ID,
				Group:     alpha.Group,
				CreatedAt: time.Now().UnixMilli(),
			}
			if err := write(ctx, betaWriter, betaKey, betaValue, beta.ID, beta); err != nil {
				fmt.Fprintf(os.Stderr, "writing to %s: %v\n", betaTopic, err)
			}
		}
	}
}

// write encodes the key and the value with their own schema and produces one message.
func write(ctx context.Context, w *kafka.Writer, keyCodec, valueCodec *codec, key string, value any) error {
	encodedKey, err := keyCodec.encode(key)
	if err != nil {
		return err
	}

	encodedValue, err := valueCodec.encode(value)
	if err != nil {
		return err
	}

	return w.WriteMessages(ctx, kafka.Message{Key: encodedKey, Value: encodedValue})
}

// register posts <subject>.avsc from the schema directory to the Schema Registry and returns a
// codec bound to the id the registry answers with. Registration is idempotent: re-posting a
// schema that is already there returns its existing id.
func register(ctx context.Context, subject string) (*codec, error) {
	path := filepath.Join(schemaDir, subject+".avsc")

	raw, err := os.ReadFile(path) //nolint:gosec // path comes from local configuration
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", path, err)
	}

	schema, err := avro.Parse(string(raw))
	if err != nil {
		return nil, fmt.Errorf("parsing %s: %w", path, err)
	}

	body, err := json.Marshal(map[string]string{"schema": string(raw)})
	if err != nil {
		return nil, fmt.Errorf("building the registration request: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt < 10; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(2 * time.Second):
			}
		}

		id, err := post(ctx, subject, body)
		if err != nil {
			lastErr = err
			fmt.Fprintf(os.Stderr, "registering %s: %v\n", subject, err)

			continue
		}

		fmt.Printf("Subject %s has schema id %d\n", subject, id)

		return &codec{id: id, schema: schema}, nil
	}

	return nil, fmt.Errorf("registering %s: %w", subject, lastErr)
}

// post sends one registration request and returns the schema id from the response.
func post(ctx context.Context, subject string, body []byte) (int, error) {
	url := fmt.Sprintf("%s/subjects/%s/versions", strings.TrimRight(schemaRegistry, "/"), subject)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/vnd.schemaregistry.v1+json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("schema registry answered %s", resp.Status)
	}

	var registered struct {
		ID int `json:"id"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&registered); err != nil {
		return 0, fmt.Errorf("decoding the response: %w", err)
	}

	return registered.ID, nil
}

func generateAlpha() Alpha {
	return Alpha{
		ID:        uuid.New().String(),
		Group:     groups[rand.Intn(len(groups))],
		Label:     labels[rand.Intn(len(labels))],
		Kind:      kinds[rand.Intn(len(kinds))],
		Region:    regions[rand.Intn(len(regions))],
		CreatedAt: time.Now().UnixMilli(),
		Amount:    float64(rand.Intn(50_000)) / 100,
	}
}

func newKafkaWriter(topic string) *kafka.Writer {
	return &kafka.Writer{
		Addr:     kafka.TCP(strings.Split(kafkaBroker, ",")...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}
}

func generateStringSlice(prefix string, count int) []string {
	result := make([]string, count)
	for i := range result {
		result[i] = fmt.Sprintf("%s%d", prefix, i+1)
	}

	return result
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

func getEnv(key, fallback string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}

	return fallback
}

func getEnvInt(key string, fallback int) int {
	if val, ok := os.LookupEnv(key); ok {
		if parsed, err := strconv.Atoi(val); err == nil {
			return parsed
		}
	}

	return fallback
}

func getEnvFloat(key string, fallback float64) float64 {
	if val, ok := os.LookupEnv(key); ok {
		if parsed, err := strconv.ParseFloat(val, 64); err == nil {
			return parsed
		}
	}

	return fallback
}
