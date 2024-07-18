package main

import (
	"database/sql"
	"log"
	"net/http"
	"os"
	"os/signal"
	"rabbitmq-consumer/config"
	"rabbitmq-consumer/internal/infrastructure/rabbitmq"
	"rabbitmq-consumer/internal/infrastructure/websocket"
	"rabbitmq-consumer/internal/repository"
	"rabbitmq-consumer/internal/service"
	"rabbitmq-consumer/pkg/logger"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	cfg := config.LoadConfig()

	logInstance, err := logger.SetupLogger(cfg.Env)
	if err != nil {
		log.Printf("Failed to set up logger: %v", err)
		os.Exit(1)
	}

	db, err := sql.Open("mysql", cfg.Database.Addr)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to connect to MySQL", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(25)
	db.SetConnMaxLifetime(5 * time.Minute)

	repo := repository.NewSMSRepository(db, logInstance.ErrorLogger)
	smsService := service.NewSMSService(repo, logInstance)

	webSocketServer := websocket.NewWebSocketServer()
	go webSocketServer.Run()

	rabbitMQConn := rabbitmq.NewAMQPConnection(cfg, smsService, logInstance, webSocketServer)
	err = rabbitMQConn.Connect()
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to connect to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer rabbitMQConn.Close()

	http.HandleFunc("/ws", webSocketServer.HandleWebSocket)
	go func() {
		log.Printf("WebSocket server started at :8080")
		if err := http.ListenAndServe(":8080", nil); err != nil {
			log.Fatalf("Failed to start WebSocket server: %v", err)
		}
	}()

	go rabbitMQConn.ConsumeMessages()

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan

	logInstance.InfoLogger.Info("Shutting down gracefully...")
	rabbitMQConn.Close()
	logInstance.InfoLogger.Info("RabbitMQ connection closed. Exiting...")
}
