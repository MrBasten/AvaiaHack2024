package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/segmentio/kafka-go"
)

const (
	kafkaTopic    = "reviews"
	kafkaBroker   = "kafka:9092"
	serverAddress = ":8080"
)

type Review struct {
	ID      int    `json:"id"`
	Content string `json:"content"`
}

var (
	conn   driver.Conn
	writer *kafka.Writer
)

func main() {
	time.Sleep(10 * time.Second)

	var err error
	conn, err = connect()
	if err != nil {
		log.Fatalf("Failed to connect to ClickHouse: %v", err)
	}
	defer conn.Close()

	writer = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{kafkaBroker},
		Topic:   kafkaTopic,
	})
	defer writer.Close()

	http.HandleFunc("/db", handleDB)
	http.HandleFunc("/kafka", handleKafka)

	log.Printf("Server started at %s", serverAddress)
	if err := http.ListenAndServe(serverAddress, nil); err != nil {
		log.Fatalf("Server failed: %v", err)
	}
}

func handleDB(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var review Review
	if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	query := "INSERT INTO reviews (id, content) VALUES (?, ?)"
	if err := conn.Exec(ctx, query, review.ID, review.Content); err != nil {
		log.Printf("Failed to insert data into ClickHouse: %v", err)
		http.Error(w, "Failed to insert data", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintln(w, "Data inserted into ClickHouse")
}

func handleKafka(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var review Review
	if err := json.NewDecoder(r.Body).Decode(&review); err != nil {
		http.Error(w, "Invalid request payload", http.StatusBadRequest)
		return
	}

	msg := kafka.Message{
		Value: []byte(review.Content),
	}
	if err := writer.WriteMessages(context.Background(), msg); err != nil {
		log.Printf("Failed to publish message to Kafka: %v", err)
		http.Error(w, "Failed to publish message", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	fmt.Fprintln(w, "Message published to Kafka")
}

func connect() (driver.Conn, error) {
	var (
		ctx       = context.Background()
		conn, err = clickhouse.Open(&clickhouse.Options{
			Addr: []string{"clickhouse:9000"},
			Auth: clickhouse.Auth{
				Database: "reviewsdb",
				Username: "user",
				Password: "password",
			},
			ClientInfo: clickhouse.ClientInfo{
				Products: []struct {
					Name    string
					Version string
				}{
					{Name: "an-example-go-client", Version: "0.1"},
				},
			},

			Debugf: func(format string, v ...interface{}) {
				fmt.Printf(format, v)
			},
		})
	)

	if err != nil {
		return nil, err
	}

	if err := conn.Ping(ctx); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("Exception [%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		}
		return nil, err
	}
	return conn, nil
}
