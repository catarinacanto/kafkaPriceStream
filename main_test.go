package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
	"io"
	"testing"
	"time"
)

type MockKafkaWriter struct {
	Messages []kafka.Message
}

type MockKafkaReader struct {
	Messages []kafka.Message
	Index    int
}

func TestCalculateAverageCourse(t *testing.T) {
	data := []ETFData{
		{ETF: "ETF1", ISIN: "ISIN1", Timestamp: time.Now(), Course: 100},
		{ETF: "ETF1", ISIN: "ISIN1", Timestamp: time.Now(), Course: 200},
		{ETF: "ETF1", ISIN: "ISIN1", Timestamp: time.Now(), Course: 300},
		{ETF: "ETF1", ISIN: "ISIN1", Timestamp: time.Now(), Course: 400},
	}

	expected := AggregatedETFData{
		ETF:         "ETF1",
		ISIN:        "ISIN1",
		Timestamp:   time.Now().UTC(),
		Course:      250, // Average of 100, 200, 300, 400
		Aggregation: "5s",
	}

	result := calculateAverageCourse(data, "5s")

	assert.Equal(t, expected.ETF, result.ETF)
	assert.Equal(t, expected.ISIN, result.ISIN)
	assert.Equal(t, expected.Aggregation, result.Aggregation)
	assert.Equal(t, expected.Course, result.Course)
}

func TestPublishETFMetrics(t *testing.T) {
	// Mock Kafka writer
	mockWriter := &MockKafkaWriter{}
	writer := kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}

	defer func() { _ = writer.Close() }()

	data := AggregatedETFData{
		ETF:         "ETF1",
		ISIN:        "ISIN1",
		Timestamp:   time.Now().UTC(),
		Course:      150.0,
		Aggregation: "5s",
	}

	msg, err := json.Marshal(data)
	assert.Nil(t, err)

	err = mockWriter.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(data.ISIN),
			Value: msg,
		},
	)
	assert.Nil(t, err)
	assert.Len(t, mockWriter.Messages, 1)
	assert.Contains(t, string(mockWriter.Messages[0].Value), `"ISIN1"`)
}

func TestConsumeETFData(t *testing.T) {
	mockReader := &MockKafkaReader{
		Messages: []kafka.Message{
			{Value: []byte(`{"etf":"ETF1","isin":"ISIN1","timestamp":"2024-11-22T10:00:00Z","course":150.0}`)},
			{Value: []byte(`{"etf":"ETF2","isin":"ISIN2","timestamp":"2024-11-22T10:01:00Z","course":200.0}`)},
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Channels to collect window data
	window5s := make(chan WindowData, 10)
	window1m := make(chan WindowData, 10)
	window1h := make(chan WindowData, 10)

	// Run consumeETFData
	go func() {
		consumeETFData(ctx, mockReader, window5s, window1m, window1h)
		close(window5s)
		close(window1m)
		close(window1h)
	}()

	time.Sleep(500 * time.Millisecond) // Allow time for processing

	// Verify that messages were processed
	assert.Len(t, window5s, 2, "Expected 2 messages in the 5s window channel")
	assert.Len(t, window1m, 2, "Expected 2 messages in the 1m window channel")
	assert.Len(t, window1h, 2, "Expected 2 messages in the 1h window channel")
}

func TestAggregateETFPrices(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	windowChan := make(chan WindowData, 10)

	windowChan <- WindowData{
		ISIN: "ISIN1",
		Data: ETFData{ETF: "ETF1", ISIN: "ISIN1", Timestamp: time.Now(), Course: 100.0},
	}
	windowChan <- WindowData{
		ISIN: "ISIN1",
		Data: ETFData{ETF: "ETF1", ISIN: "ISIN1", Timestamp: time.Now(), Course: 200.0},
	}

	go aggregateETFPrices(ctx, windowChan, 1*time.Second, "1s")

	// Allow time for aggregation
	time.Sleep(2 * time.Second)

	// Verify that the channel is drained
	assert.Len(t, windowChan, 0, "Expected window channel to be empty after aggregation")
}

func (m *MockKafkaWriter) WriteMessages(ctx context.Context, msgs ...kafka.Message) error {
	m.Messages = append(m.Messages, msgs...)
	return nil
}

func (m *MockKafkaWriter) Close() error {
	return nil
}

func (m *MockKafkaReader) ReadMessage(ctx context.Context) (kafka.Message, error) {
	if m.Index >= len(m.Messages) {
		return kafka.Message{}, io.EOF // Simulate end of the stream
	}
	msg := m.Messages[m.Index]
	m.Index++
	return msg, nil
}

func (m *MockKafkaReader) Close() error {
	return nil
}
