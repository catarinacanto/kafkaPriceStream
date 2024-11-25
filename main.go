package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"io"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type ETFData struct {
	ETF       string    `json:"etf"`
	ISIN      string    `json:"isin"`
	Timestamp time.Time `json:"timestamp"`
	Course    float64   `json:"course"`
}

type AggregatedETFData struct {
	ETF         string    `json:"etf"`
	ISIN        string    `json:"isin"`
	Timestamp   time.Time `json:"timestamp"`
	Course      float64   `json:"course"`
	Aggregation string    `json:"aggregation"`
}

type WindowData struct {
	ISIN string
	Data ETFData
}

var (
	writer       *kafka.Writer
	mu           sync.Mutex
	window5sChan = make(chan WindowData, 100)
	window1mChan = make(chan WindowData, 100)
	window1hChan = make(chan WindowData, 100)
	kafkaTopic   = "aggregated-etf-prices"
)

const kafkaBroker = "localhost:9092"
const (
	windowSize5s = 5 * time.Second
	windowSize1m = 1 * time.Minute
	windowSize1h = 1 * time.Hour
)

type KafkaReader interface {
	ReadMessage(ctx context.Context) (kafka.Message, error)
	Close() error
}

func init() {
	writer = &kafka.Writer{
		Addr:     kafka.TCP(kafkaBroker),
		Topic:    kafkaTopic,
		Balancer: &kafka.LeastBytes{},
	}
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create a WaitGroup to manage goroutines
	var wg sync.WaitGroup

	// Capture shutdown signals
	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
		<-sigChan
		log.Println("Shutdown signal received, exiting...")
		cancel()
	}()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{kafkaBroker},
		Topic:   "etf-prices",
		GroupID: "etf-aggregator-group",
	})

	wg.Add(1)
	go func() {
		defer wg.Done()
		consumeETFData(ctx, reader, window5sChan, window1mChan, window1hChan)
	}()

	wg.Add(3) // One for each window size
	go func() {
		defer wg.Done()
		aggregateETFPrices(ctx, window5sChan, windowSize5s, "5s")
	}()
	go func() {
		defer wg.Done()
		aggregateETFPrices(ctx, window1mChan, windowSize1m, "1m")
	}()
	go func() {
		defer wg.Done()
		aggregateETFPrices(ctx, window1hChan, windowSize1h, "1h")
	}()

	// Wait for all goroutines to finish before shutting down
	wg.Wait()

	// Graceful shutdown: Closing Kafka writer
	log.Println("Shutting down Kafka writer...")
	if err := writer.Close(); err != nil {
		log.Printf("Error closing Kafka writer: %v", err)
	} else {
		log.Println("Kafka writer closed successfully.")
	}

	log.Println("Application shutdown complete")
}

func consumeETFData(ctx context.Context, reader KafkaReader, window5sChan, window1mChan, window1hChan chan WindowData) {
	defer func() {
		if err := reader.Close(); err != nil {
			log.Printf("Error closing Kafka reader: %v", err)
		}
	}()

	for {
		select {
		case <-ctx.Done():
			log.Println("Context canceled, stopping Kafka consumer")
			return
		default:
			msg, err := reader.ReadMessage(ctx)
			if err != nil {
				if err == io.EOF {
					log.Println("End of Kafka message stream")
					return
				}
				log.Printf("Failed to read message: %v", err)
				continue
			}

			if len(msg.Value) == 0 {
				log.Println("Skipping empty message")
				continue
			}

			var data ETFData
			if err := json.Unmarshal(msg.Value, &data); err != nil {
				log.Printf("Failed to unmarshal message: %v", err)
				continue
			}

			window5sChan <- WindowData{ISIN: data.ISIN, Data: data}
			window1mChan <- WindowData{ISIN: data.ISIN, Data: data}
			window1hChan <- WindowData{ISIN: data.ISIN, Data: data}
		}
	}
}

func aggregateETFPrices(ctx context.Context, windowChan <-chan WindowData, interval time.Duration, bucket string) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	window := make(map[string][]ETFData)

	for {
		select {
		case <-ctx.Done():
			log.Printf("Context canceled, stopping aggregation for bucket %s", bucket)
			return
		case <-ticker.C:
			mu.Lock()
			for isin, data := range window {
				if len(data) == 0 {
					continue
				}

				averageCourse := calculateAverageCourse(data, bucket)

				// Emit and track the processed result
				publishETFMetrics(averageCourse)

				delete(window, isin)
			}
			mu.Unlock()
		case wd := <-windowChan:
			mu.Lock()
			window[wd.ISIN] = append(window[wd.ISIN], wd.Data)
			mu.Unlock()
		}
	}
}

func calculateAverageCourse(data []ETFData, bucket string) AggregatedETFData {
	var total float64
	for _, d := range data {
		total += d.Course
	}
	avg := total / float64(len(data))
	lastMessage := data[len(data)-1]

	return AggregatedETFData{
		ETF:         lastMessage.ETF,
		ISIN:        lastMessage.ISIN,
		Timestamp:   time.Now().UTC(),
		Course:      avg,
		Aggregation: bucket,
	}
}

func publishETFMetrics(data AggregatedETFData) {
	msg, err := json.Marshal(data)
	if err != nil {
		log.Printf("Failed to marshal aggregated etf data: %v", err)
		return
	}

	err = writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(data.ISIN),
			Value: msg,
		},
	)
	if err != nil {
		log.Printf("Failed to write message to Kafka: %v", err)
	}
}
