package service

import (
	"encoding/json"
	"log/slog"
	"rabbitmq-consumer/internal/delivery/websocket"
	"rabbitmq-consumer/internal/domain"
	"rabbitmq-consumer/internal/repository"
	"rabbitmq-consumer/pkg/logger"

	"github.com/streadway/amqp"
)

type SMSService interface {
	ProcessMessage(body []byte) (domain.SMSMessage, error)
	HandleMessages(msgs <-chan amqp.Delivery)
}

type smsService struct {
	repo     repository.SMSRepository
	logger   *logger.Loggers
	wsServer *websocket.WebSocketServer
}

func NewSMSService(repo repository.SMSRepository, logger *logger.Loggers, wsServer *websocket.WebSocketServer) SMSService {
	return &smsService{repo: repo, logger: logger, wsServer: wsServer}
}

func (s *smsService) ProcessMessage(body []byte) (domain.SMSMessage, error) {
	var msg domain.SMSMessage
	err := json.Unmarshal(body, &msg)
	if err != nil {
		s.logger.ErrorLogger.Error("Failed to unmarshal message", slog.Any("error", err))
		return domain.SMSMessage{}, err
	}

	if msg.Date == "" {
		s.logger.ErrorLogger.Error("Failed to parse message: date is missing", slog.String("message", string(body)))
		return domain.SMSMessage{}, err
	}

	userID, err := s.repo.FindUserID(msg.Destination)
	if err != nil {
		return domain.SMSMessage{}, err
	}

	clientID, err := s.repo.FindOrCreateClientID(msg.Source)
	if err != nil {
		return domain.SMSMessage{}, err
	}

	err = s.repo.InsertMessage(msg, clientID, userID)
	if err != nil {
		return domain.SMSMessage{}, err
	}

	s.logger.InfoLogger.Info("Message recorded in database",
		slog.String("src", msg.Source),
		slog.String("dst", msg.Destination),
		slog.String("txt", msg.Text),
		slog.String("date", msg.Date),
		slog.Int("parts", msg.Parts),
	)

	return msg, nil
}

func (s *smsService) HandleMessages(msgs <-chan amqp.Delivery) {
	for d := range msgs {
		msg, err := s.ProcessMessage(d.Body)
		if err != nil {
			continue
		}
		s.wsServer.BroadcastMessage(msg)
	}
}
