package config

import (
	"log"

	"github.com/ilyakaznacheev/cleanenv"
)

type Config struct {
	Env       string    `yaml:"env"`
	Database  Database  `yaml:"database"`
	RabbitMQ  RabbitMQ  `yaml:"rabbitmq"`
	WebSocket WebSocket `yaml:"websocket"`
}

type Database struct {
	Addr string `yaml:"address"`
}

type RabbitMQ struct {
	URL      string      `yaml:"url"`
	Consumer ConsumerKey `yaml:"consumer"`
}

type ConsumerKey struct {
	ExchangeName string `yaml:"exchange_name"`
	QueueName    string `yaml:"queue_name"`
	RoutingKey   string `yaml:"routing_key"`
}

type WebSocket struct {
	Addr string `yaml:"address"`
}

func LoadConfig() *Config {
	configPath := "config.yaml"

	if configPath == "" {
		log.Fatalf("config path is not set or config file does not exist")
	}

	var cfg Config

	if err := cleanenv.ReadConfig(configPath, &cfg); err != nil {
		log.Fatalf("Cannot read config: %v", err)
	}

	return &cfg
}
