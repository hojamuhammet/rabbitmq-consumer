package main

import (
	"database/sql"
	"log/slog"
	"os"
	"rabbitmq-consumer/config"
	rabbitmq "rabbitmq-consumer/internal/infrastructure"
	"rabbitmq-consumer/internal/repository"
	"rabbitmq-consumer/internal/service"
	"rabbitmq-consumer/pkg/logger"

	_ "github.com/go-sql-driver/mysql"
)

func main() {
	cfg := config.LoadConfig()

	logInstance, err := logger.SetupLogger(cfg.Env)
	if err != nil {
		slog.Error("failed to set up logger", "error", err)
		os.Exit(1)
	}

	db, err := sql.Open("mysql", cfg.Database.Addr)
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to connect to MySQL", "error", err)
		os.Exit(1)
	}
	defer db.Close()

	repo := repository.NewSMSRepository(db, logInstance.ErrorLogger)
	smsService := service.NewSMSService(repo, logInstance)

	rabbitMQConn := rabbitmq.NewAMQPConnection(cfg, smsService, logInstance)
	err = rabbitMQConn.Connect()
	if err != nil {
		logInstance.ErrorLogger.Error("Failed to connect to RabbitMQ", "error", err)
		os.Exit(1)
	}
	defer rabbitMQConn.Close()

	rabbitMQConn.ConsumeMessages()

	select {}
}
