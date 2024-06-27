package main

import (
	"database/sql"
	"log"
	"log/slog"
	"os"
	"rabbitmq-consumer/config"
	"rabbitmq-consumer/pkg/logger"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
)

var (
	rabbitMQConn *amqp.Connection
	logInstance  *logger.Loggers
	db           *sql.DB
)

func main() {
	cfg := config.LoadConfig()

	var err error
	logInstance, err = logger.SetupLogger(cfg.Env)
	if err != nil {
		slog.Error("failed to set up logger: %v", err)
		os.Exit(1)
	}

	logInstance.InfoLogger.Info("Server is up and running")

	rabbitMQConn, err = amqp.Dial(cfg.RabbitMQ.URL)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer rabbitMQConn.Close()

	db, err = sql.Open("mysql", cfg.Database.Addr)
	if err != nil {
		log.Fatalf("Failed to connect to MySQL: %v", err)
	}
	defer db.Close()

	consumeMessages()
}

func consumeMessages() {
	channel, err := rabbitMQConn.Channel()
	if err != nil {
		log.Fatalf("Failed to open a channel: %v", err)
	}
	defer channel.Close()

	queue, err := channel.QueueDeclare(
		"extra.turkmentv", // name of the queue
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare a queue: %v", err)
	}

	msgs, err := channel.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		log.Fatalf("Failed to register a consumer: %v", err)
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %s", d.Body)
			processMessage(d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

func processMessage(body []byte) {
	message := string(body)

	// Split the message into parts based on ", "
	parts := strings.Split(message, ", ")
	if len(parts) < 3 {
		log.Fatalf("Failed to parse message: input does not match format")
	}

	// Extract source, destination, and text from parts
	var source, destination, text string
	for _, part := range parts {
		switch {
		case strings.HasPrefix(part, "src="):
			source = strings.TrimPrefix(part, "src=")
		case strings.HasPrefix(part, "dst="):
			destination = strings.TrimPrefix(part, "dst=")
		case strings.HasPrefix(part, "txt="):
			textPart := strings.TrimPrefix(part, "txt=")
			if text != "" {
				text += ", "
			}
			text += textPart
		default:
			log.Fatalf("Failed to parse message: unrecognized part format")
		}
	}

	// Check if the user already exists
	var userID sql.NullInt64
	err := db.QueryRow("SELECT id FROM users WHERE login = ?", destination).Scan(&userID)
	if err != nil {
		if err == sql.ErrNoRows {
			// User does not exist, so create a new user
			_, err := db.Exec(
				"INSERT INTO users (login) VALUES (?)",
				destination,
			)
			if err != nil {
				log.Fatalf("Failed to insert new user: %v", err)
			}
		} else {
			log.Fatalf("Failed to query user: %v", err)
		}
	}

	// Insert the message into the sms_messages table
	_, err = db.Exec(
		"INSERT INTO sms_messages (dt, msg, client) VALUES (?, ?, ?)",
		time.Now(), text, source,
	)
	if err != nil {
		log.Fatalf("Failed to insert message into sms_messages table: %v", err)
	}

	log.Printf("Message recorded in database: src=%s, dst=%s, txt=%s", source, destination, text)
}
