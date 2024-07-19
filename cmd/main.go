package main

import (
	"database/sql"
	"net/http"
	"os"
	"os/signal"
	"rabbitmq-consumer/config"
	"rabbitmq-consumer/internal/delivery/websocket"
	rabbitmq "rabbitmq-consumer/internal/infrastructure"
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
	webSocketServer := websocket.NewWebSocketServer()
	smsService := service.NewSMSService(repo, logInstance, webSocketServer)

	go webSocketServer.Run()

	rabbitMQConn := rabbitmq.NewAMQPConnection(cfg, smsService, logInstance, webSocketServer)
	err = rabbitMQConn.Connect()
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to connect to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer rabbitMQConn.Close()

	msgs, err := rabbitMQConn.ConsumeMessages()
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to start consuming messages", "error", err)
		os.Exit(1)
	}

	go smsService.HandleMessages(msgs)

	server := &http.Server{
		Addr:    cfg.WebSocket.Addr,
		Handler: http.DefaultServeMux,
	}

	http.HandleFunc("/msg-ws", webSocketServer.HandleWebSocket)
	go func() {
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logInstance.ErrorLogger.Error("Failed to start WebSocket server", "error", err)
		}
	}()

	// Graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	<-sigChan
	logInstance.InfoLogger.Info("Shutting down gracefully...")

	rabbitMQConn.Close()
	logInstance.InfoLogger.Info("RabbitMQ connection closed.")

	if err := server.Close(); err != nil {
		logInstance.ErrorLogger.Error("Failed to close WebSocket server", "error", err)
	} else {
		logInstance.InfoLogger.Info("WebSocket server closed.")
	}

	time.Sleep(1 * time.Second)

	logInstance.InfoLogger.Info("Exiting...")
	os.Exit(0)
}
