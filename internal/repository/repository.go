package repository

import (
	"database/sql"
	"log"
	"log/slog"
	"rabbitmq-consumer/internal/domain"
	"time"
)

type SMSRepository interface {
	FindUserID(destination string) (int64, error)
	FindOrCreateClientID(source string) (int64, error)
	InsertMessage(msg domain.SMSMessage, clientID, userID int64) error
}

type smsRepository struct {
	db     *sql.DB
	logger *slog.Logger
}

func NewSMSRepository(db *sql.DB, logger *slog.Logger) SMSRepository {
	return &smsRepository{db: db, logger: logger}
}

func (r *smsRepository) FindUserID(destination string) (int64, error) {
	var userID int64
	err := r.db.QueryRow("SELECT id FROM users WHERE login = ?", destination).Scan(&userID)
	if err != nil {
		if err == sql.ErrNoRows {
			r.logger.Error("Failed to find user with login", slog.String("login", destination))
			log.Printf("Failed to find user with login: %s", destination) // Log to console
		} else {
			r.logger.Error("Failed to query user", slog.Any("error", err))
			log.Printf("Failed to query user: %v", err) // Log to console
		}
		return 0, err
	}
	return userID, nil
}

func (r *smsRepository) FindOrCreateClientID(source string) (int64, error) {
	var clientID int64
	err := r.db.QueryRow("SELECT id FROM clients WHERE phone = ?", source).Scan(&clientID)
	if err != nil {
		if err == sql.ErrNoRows {
			res, err := r.db.Exec("INSERT INTO clients (phone) VALUES (?)", source)
			if err != nil {
				r.logger.Error("Failed to insert client into clients table", slog.Any("error", err))
				log.Printf("Failed to insert client into clients table: %v", err) // Log to console
				return 0, err
			}
			clientID, err = res.LastInsertId()
			if err != nil {
				r.logger.Error("Failed to get last insert id for clients", slog.Any("error", err))
				log.Printf("Failed to get last insert id for clients: %v", err) // Log to console
				return 0, err
			}
		} else {
			r.logger.Error("Failed to query client", slog.Any("error", err))
			log.Printf("Failed to query client: %v", err) // Log to console
			return 0, err
		}
	}
	return clientID, nil
}

func (r *smsRepository) InsertMessage(msg domain.SMSMessage, clientID, userID int64) error {
	parsedDate, err := time.Parse("2006-01-02T15:04:05", msg.Date)
	if err != nil {
		r.logger.Error("Failed to parse date", slog.String("date", msg.Date), slog.Any("error", err))
		log.Printf("Failed to parse date: %s, error: %v", msg.Date, err) // Log to console
		return err
	}

	_, err = r.db.Exec(
		"INSERT INTO sms_messages (dt, msg, client_id, user_id, parts) VALUES (?, ?, ?, ?, ?)",
		parsedDate, msg.Text, clientID, userID, msg.Parts,
	)
	if err != nil {
		r.logger.Error("Failed to insert message into sms_messages table", slog.Any("error", err))
		log.Printf("Failed to insert message into sms_messages table: %v", err) // Log to console
		return err
	}

	return nil
}
