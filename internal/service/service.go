package service

import (
	"encoding/json"
	"log"
	"log/slog"
	"rabbitmq-consumer/internal/domain"
	"rabbitmq-consumer/internal/repository"
	"rabbitmq-consumer/pkg/logger"
)

type SMSService interface {
	ProcessMessage(body []byte) error
}

type smsService struct {
	repo   repository.SMSRepository
	logger *logger.Loggers
}

func NewSMSService(repo repository.SMSRepository, logger *logger.Loggers) SMSService {
	return &smsService{repo: repo, logger: logger}
}

func (s *smsService) ProcessMessage(body []byte) error {
	var msg domain.SMSMessage
	err := json.Unmarshal(body, &msg)
	if err != nil {
		s.logger.ErrorLogger.Error("Failed to unmarshal message", slog.Any("error", err))
		log.Printf("Failed to unmarshal message: %v", err) // Log to console
		return err
	}

	if msg.Date == "" {
		s.logger.ErrorLogger.Error("Failed to parse message: date is missing", slog.String("message", string(body)))
		log.Printf("Failed to parse message: date is missing, message: %s", string(body)) // Log to console
		return err
	}

	userID, err := s.repo.FindUserID(msg.Destination)
	if err != nil {
		return err
	}

	clientID, err := s.repo.FindOrCreateClientID(msg.Source)
	if err != nil {
		return err
	}

	err = s.repo.InsertMessage(msg, clientID, userID)
	if err != nil {
		return err
	}

	s.logger.InfoLogger.Info("Message recorded in database",
		slog.String("src", msg.Source),
		slog.String("dst", msg.Destination),
		slog.String("txt", msg.Text),
		slog.String("date", msg.Date),
		slog.Int("parts", msg.Parts),
	)
	log.Printf("Message recorded in database: src: %v, dst: %v, txt: %v, date: %v, parts: %v",
		msg.Source, msg.Destination, msg.Text, msg.Date, msg.Parts) // Log to console
	return nil
}
