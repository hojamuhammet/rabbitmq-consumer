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
		slog.Error("failed to set up logger", "error", err)
		os.Exit(1)
	}

	log.Println("Server is up and running")

	rabbitMQConn, err = amqp.Dial(cfg.RabbitMQ.URL)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to connect to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer rabbitMQConn.Close()

	db, err = sql.Open("mysql", cfg.Database.Addr)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to connect to MySQL", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	consumeMessages()
}

func consumeMessages() {
	channel, err := rabbitMQConn.Channel()
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to open a channel", "error", err)
		return
	}
	defer channel.Close()

	err = channel.ExchangeDeclare(
		"extra.turkmentv", // name
		"direct",          // type
		true,              // durable
		false,             // auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to declare an exchange", "error", err)
		return
	}

	queue, err := channel.QueueDeclare(
		"extra.turkmentv", // name of the queue
		true,              // durable
		false,             // delete when unused
		false,             // exclusive
		false,             // no-wait
		nil,               // arguments
	)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to declare a queue", "error", err)
		return
	}

	err = channel.QueueBind(
		queue.Name,        // queue name
		"",                // routing key
		"extra.turkmentv", // exchange
		false,
		nil,
	)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to bind queue to exchange", "error", err)
		return
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
		logInstance.ErrorLogger.Error("Failed to register a consumer", "error", err)
		return
	}

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			log.Printf("Received a message: %v", string(d.Body))
			processMessage(d.Body)
		}
	}()

	<-forever
}
func processMessage(body []byte) {
	message := string(body)

	parts := strings.Split(message, ", ")
	if len(parts) < 4 { // Ensure there are at least 4 parts: src, dst, txt, date
		logInstance.ErrorLogger.Error("Failed to parse message: input does not match format", "message", message)
		return
	}

	var source, destination, text, date string
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
		case strings.HasPrefix(part, "date="):
			date = strings.TrimPrefix(part, "date=")
		default:
			logInstance.ErrorLogger.Error("Failed to parse message: unrecognized part format", "part", part)
			return
		}
	}

	if date == "" {
		logInstance.ErrorLogger.Error("Failed to parse message: date is missing", "message", message)
		return
	}

	var userID int64
	err := db.QueryRow("SELECT id FROM users WHERE login = ?", destination).Scan(&userID)
	if err != nil {
		if err == sql.ErrNoRows {
			logInstance.ErrorLogger.Error("Failed to find user with login", "login", destination)
			return
		} else {
			logInstance.ErrorLogger.Error("Failed to query user", "error", err)
			return
		}
	}

	parsedDate, err := time.Parse(time.RFC3339, date)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to parse date", "date", date, "error", err)
		return
	}

	_, err = db.Exec(
		"INSERT INTO sms_messages (dt, msg, client, user_id) VALUES (?, ?, ?, ?)",
		parsedDate, text, source, userID,
	)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to insert message into sms_messages table", "error", err)
		return
	}

	log.Printf("Message recorded in database: src: %v, dst: %v, txt: %v, date: %v", source, destination, text, parsedDate)
}
