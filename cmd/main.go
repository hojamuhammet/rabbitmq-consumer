package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"log/slog"
	"os"
	"rabbitmq-consumer/config"
	"rabbitmq-consumer/pkg/logger"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/streadway/amqp"
)

var (
	rabbitMQConn *amqp.Connection
	logInstance  *logger.Loggers
	db           *sql.DB
)

type SMSMessage struct {
	Source      string `json:"src"`
	Destination string `json:"dst"`
	Text        string `json:"txt"`
	Date        string `json:"date"`
	Parts       int    `json:"parts"`
}

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
	var msg SMSMessage
	err := json.Unmarshal(body, &msg)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to unmarshal message", "error", err)
		return
	}

	log.Printf("Processing message: %+v", msg)

	if msg.Date == "" {
		logInstance.ErrorLogger.Error("Failed to parse message: date is missing", "message", string(body))
		return
	}

	var userID int64
	err = db.QueryRow("SELECT id FROM users WHERE login = ?", msg.Destination).Scan(&userID)
	if err != nil {
		if err == sql.ErrNoRows {
			logInstance.ErrorLogger.Error("Failed to find user with login", "login", msg.Destination)
			return
		} else {
			logInstance.ErrorLogger.Error("Failed to query user", "error", err)
			return
		}
	}

	var clientID int64
	err = db.QueryRow("SELECT id FROM clients WHERE phone = ?", msg.Source).Scan(&clientID)
	if err != nil {
		if err == sql.ErrNoRows {
			res, err := db.Exec("INSERT INTO clients (phone) VALUES (?)", msg.Source)
			if err != nil {
				logInstance.ErrorLogger.Error("Failed to insert client into clients table", "error", err)
				return
			}
			clientID, err = res.LastInsertId()
			if err != nil {
				logInstance.ErrorLogger.Error("Failed to get last insert id for clients", "error", err)
				return
			}
		} else {
			logInstance.ErrorLogger.Error("Failed to query client", "error", err)
			return
		}
	}

	parsedDate, err := time.Parse("2006-01-02T15:04:05", msg.Date)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to parse date", "date", msg.Date, "error", err)
		return
	}

	_, err = db.Exec(
		"INSERT INTO sms_messages (dt, msg, client_id, user_id, parts) VALUES (?, ?, ?, ?, ?)",
		parsedDate, msg.Text, clientID, userID, msg.Parts,
	)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to insert message into sms_messages table", "error", err)
		log.Printf("INSERT INTO sms_messages (dt, msg, client_id, user_id, parts) VALUES (%v, %v, %v, %v, %v)", parsedDate, msg.Text, clientID, userID, msg.Parts)
		return
	}

	log.Printf("Message recorded in database: src: %v, dst: %v, txt: %v, date: %v, parts: %v", msg.Source, msg.Destination, msg.Text, parsedDate, msg.Parts)
}
