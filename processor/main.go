package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	_ "github.com/mattn/go-sqlite3"
)

// --- Configuration Constants ---
const (
	ProjectID          = "test-project"
	SubscriptionID     = "scan-sub"
	EmulatorHostEnvVar = "PUBSUB_EMULATOR_HOST"
	DatabaseFile       = "scan_records.db"

	// Retry constants for subscription existence check
	MaxSubscriptionRetries = 10
	SubscriptionRetryDelay = 2 * time.Second
)

// --- Data Structures ---

// incoming Pub/Sub message data.
// can have different 'data_version' fields.
type ScanMessage struct {
	DataVersion int    `json:"data_version"`
	IP          string `json:"ip"`
	Port        int    `json:"port"`
	Service     string `json:"service"`
	Data        struct {
		ResponseBytesUTF8 string `json:"response_bytes_utf8"` // For data_version 1 (base64 encoded)
		ResponseStr       string `json:"response_str"`        // For data_version 2 (plain string)
	} `json:"data"`
	// Add other fields if needed for future use.
}

// data to be stored in the database.
type ScanRecord struct {
	IP              string
	Port            int
	Service         string
	LastScanned     int64 //  timestamp
	ServiceResponse string
}

// --- Database Operations ---

// initDB initializes the SQLite database and creates the necessary table.
// It uses a composite primary key (ip, port, service) to ensure uniqueness.
func initDB() (*sql.DB, error) {
	db, err := sql.Open("sqlite3", DatabaseFile)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	// Create table if it doesn't exist.
	// The UNIQUE constraint on (ip, port, service) ensures that
	// subsequent inserts with the same combination will cause a conflict,
	// which we'll handle with UPSERT.
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS scan_records (
		ip TEXT NOT NULL,
		port INTEGER NOT NULL,
		service TEXT NOT NULL,
		last_scanned INTEGER NOT NULL,
		service_response TEXT NOT NULL,
		PRIMARY KEY (ip, port, service)
	);`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create table: %w", err)
	}

	log.Printf("Database initialized: %s", DatabaseFile)
	return db, nil
}

// inserts or updates a scan record in the database.
// It uses an UPSERT (INSERT OR REPLACE) strategy to ensure "at-least-once"
// If a record with the same (ip, port, service) exists, it will be updated.
func upsertScanRecord(db *sql.DB, record ScanRecord) error {
	// INSERT OR REPLACE will atomically replace the existing row if the PRIMARY KEY conflicts.
	// This ensures the record is always "up-to-date" with the latest scan.
	insertSQL := `
	INSERT OR REPLACE INTO scan_records (ip, port, service, last_scanned, service_response)
	VALUES (?, ?, ?, ?, ?);`

	_, err := db.Exec(insertSQL, record.IP, record.Port, record.Service, record.LastScanned, record.ServiceResponse)
	if err != nil {
		return fmt.Errorf("failed to upsert scan record for %s:%d/%s: %w", record.IP, record.Port, record.Service, err)
	}
	return nil
}

// --- Pub/Sub Consumer Logic ---

// parse the incoming Pub/Sub message data and convert it to a ScanRecord.
// It handles the two specified data_version formats.
func processMessage(msgData []byte) (ScanRecord, error) {
	var scanMsg ScanMessage
	err := json.Unmarshal(msgData, &scanMsg)
	if err != nil {
		return ScanRecord{}, fmt.Errorf("failed to unmarshal message JSON: %w", err)
	}

	var serviceResponse string
	switch scanMsg.DataVersion {
	case 1:
		// Decode base64 for data_version 1
		decodedBytes, err := base64.StdEncoding.DecodeString(scanMsg.Data.ResponseBytesUTF8)
		if err != nil {
			return ScanRecord{}, fmt.Errorf("failed to base64 decode response for %s:%d/%s: %w", scanMsg.IP, scanMsg.Port, scanMsg.Service, err)
		}
		serviceResponse = string(decodedBytes)
	case 2:
		// Use directly for data_version 2
		serviceResponse = scanMsg.Data.ResponseStr
	default:
		return ScanRecord{}, fmt.Errorf("unsupported data_version: %d for %s:%d/%s", scanMsg.DataVersion, scanMsg.IP, scanMsg.Port, scanMsg.Service)
	}

	// Create the ScanRecord
	record := ScanRecord{
		IP:              scanMsg.IP,
		Port:            scanMsg.Port,
		Service:         scanMsg.Service,
		LastScanned:     time.Now().Unix(),
		ServiceResponse: serviceResponse,
	}

	return record, nil
}

// set up and run the consumer
func main() {
	// Set up logging
	log.SetOutput(os.Stdout)
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)

	log.Println("Starting Censys Scan Data Processor...")

	// --- 1. Initialize Database ---
	db, err := initDB()
	if err != nil {
		log.Fatalf("Database initialization error: %v", err)
	}
	defer db.Close()

	// --- 2. Initialize Pub/Sub Client ---
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Check for PUBSUB_EMULATOR_HOST environment variable
	if os.Getenv(EmulatorHostEnvVar) == "" {
		log.Fatalf("Environment variable %s is not set. Please ensure the Pub/Sub emulator is running and configured.", EmulatorHostEnvVar)
	}

	client, err := pubsub.NewClient(ctx, ProjectID)
	if err != nil {
		log.Fatalf("Failed to create Pub/Sub client: %v", err)
	}
	defer client.Close()

	sub := client.Subscription(SubscriptionID)

	// --- Retry loop for subscription existence ---
	for i := 0; i < MaxSubscriptionRetries; i++ {
		exists, err := sub.Exists(ctx)
		if err != nil {
			log.Printf("Attempt %d/%d: Failed to check if subscription exists: %v. Retrying in %v...",
				i+1, MaxSubscriptionRetries, err, SubscriptionRetryDelay)
			time.Sleep(SubscriptionRetryDelay)
			continue
		}
		if !exists {
			log.Printf("Attempt %d/%d: Subscription '%s' does not exist yet. Retrying in %v...",
				i+1, MaxSubscriptionRetries, SubscriptionID, SubscriptionRetryDelay)
			time.Sleep(SubscriptionRetryDelay)
			continue
		}
		log.Printf("Connected to Pub/Sub project '%s', subscription '%s'", ProjectID, SubscriptionID)
		break // Subscription found, exit loop
	}

	// Final check after retry attempts
	finalExists, err := sub.Exists(ctx)
	if err != nil {
		log.Fatalf("Failed to check if subscription exists after retries: %v", err)
	}
	if !finalExists {
		log.Fatalf("Subscription '%s' still does not exist after %d attempts. Please ensure 'docker compose up' has run successfully and the scanner has created the subscription.", SubscriptionID, MaxSubscriptionRetries)
	}

	// --- 3. Start Message Consumption ---
	log.Println("Starting to pull messages from Pub/Sub...")
	// Receive messages in a goroutine to allow graceful shutdown
	go func() {
		err := sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
			log.Printf("Received message ID: %s", msg.ID)

			// Process the message data
			record, err := processMessage(msg.Data)
			if err != nil {
				log.Printf("Error processing message ID %s: %v. Nacking message.", msg.ID, err)
				msg.Nack() // Nack the message so it can be redelivered
				return
			}

			// Upsert the record into the database
			err = upsertScanRecord(db, record)
			if err != nil {
				log.Printf("Error upserting record from message ID %s (%s:%d/%s): %v. Nacking message.",
					msg.ID, record.IP, record.Port, record.Service, err)
				msg.Nack() // Nack the message so it can be redelivered
				return
			}

			log.Printf("Successfully processed and stored record for %s:%d/%s (Message ID: %s)",
				record.IP, record.Port, record.Service, msg.ID)
			msg.Ack() // Acknowledge the message only after successful processing and storage
		})

		if err != nil && err != context.Canceled {
			log.Fatalf("Pub/Sub Receive error: %v", err)
		}
		log.Println("Pub/Sub receiver stopped.")
	}()

	// --- 4. Graceful Shutdown ---
	// Listen for OS signals to gracefully shut down
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	s := <-sigChan // Block until a signal is received
	log.Printf("Received signal: %s. Shutting down...", s)

	// Cancel the context to stop the Pub/Sub receiver
	cancel()

	// Give some time for the receiver to stop (optional, sub.Receive blocks until context is done)
	time.Sleep(2 * time.Second)

	log.Println("Application gracefully shut down.")
}
