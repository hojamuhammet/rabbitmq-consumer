package rabbitmq

import (
	"rabbitmq-consumer/config"
	"rabbitmq-consumer/internal/delivery/websocket"
	"rabbitmq-consumer/internal/service"
	"rabbitmq-consumer/pkg/logger"
	"time"

	"github.com/streadway/amqp"
)

type AMQPConnection struct {
	conn            *amqp.Connection
	channel         *amqp.Channel
	notifyConnClose chan *amqp.Error
	notifyChanClose chan *amqp.Error
	done            chan bool
	cfg             *config.Config
	smsService      service.SMSService
	logInstance     *logger.Loggers
	wsServer        *websocket.WebSocketServer
}

func NewAMQPConnection(cfg *config.Config, smsService service.SMSService, logInstance *logger.Loggers, wsServer *websocket.WebSocketServer) *AMQPConnection {
	return &AMQPConnection{
		cfg:         cfg,
		done:        make(chan bool),
		smsService:  smsService,
		logInstance: logInstance,
		wsServer:    wsServer,
	}
}

func (c *AMQPConnection) Connect() error {
	var err error

	c.conn, err = amqp.Dial(c.cfg.RabbitMQ.URL)
	if err != nil {
		return err
	}

	c.channel, err = c.conn.Channel()
	if err != nil {
		return err
	}

	c.notifyConnClose = c.conn.NotifyClose(make(chan *amqp.Error))
	c.notifyChanClose = c.channel.NotifyClose(make(chan *amqp.Error))

	go c.handleReconnection()

	return nil
}

func (c *AMQPConnection) handleReconnection() {
	for {
		select {
		case <-c.notifyConnClose:
			c.reconnect()
		case <-c.notifyChanClose:
			c.reconnect()
		case <-c.done:
			return
		}
	}
}

func (c *AMQPConnection) reconnect() {
	for {
		err := c.Connect()
		if err == nil {
			c.ConsumeMessages()
			return
		}
		time.Sleep(5 * time.Second)
	}
}

func (c *AMQPConnection) Close() {
	c.done <- true
	if !c.conn.IsClosed() {
		c.channel.Close()
		c.conn.Close()
	}
}

func (c *AMQPConnection) ConsumeMessages() (<-chan amqp.Delivery, error) {
	routingKey := c.cfg.RabbitMQ.Consumer.RoutingKey

	// Declare exchange and queue for "extra.turkmentv"
	err := c.channel.ExchangeDeclare(
		c.cfg.RabbitMQ.Consumer.ExchangeName, // name
		"direct",                             // type
		true,                                 // durable
		false,                                // auto-deleted
		false,                                // internal
		false,                                // no-wait
		nil,                                  // arguments
	)
	if err != nil {
		c.logInstance.ErrorLogger.Error("Failed to declare an exchange", "error", err)
		return nil, err
	}

	queue, err := c.channel.QueueDeclare(
		c.cfg.RabbitMQ.Consumer.QueueName, // name of the queue
		true,                              // durable
		false,                             // delete when unused
		false,                             // exclusive
		false,                             // no-wait
		nil,                               // arguments
	)
	if err != nil {
		c.logInstance.ErrorLogger.Error("Failed to declare a queue", "error", err)
		return nil, err
	}

	err = c.channel.QueueBind(
		queue.Name,                           // queue name
		routingKey,                           // routing key
		c.cfg.RabbitMQ.Consumer.ExchangeName, // exchange
		false,
		nil,
	)
	if err != nil {
		c.logInstance.ErrorLogger.Error("Failed to bind queue to exchange", "error", err)
		return nil, err
	}

	msgs, err := c.channel.Consume(
		queue.Name, // queue
		"",         // consumer
		true,       // auto-ack
		false,      // exclusive
		false,      // no-local
		false,      // no-wait
		nil,        // args
	)
	if err != nil {
		c.logInstance.ErrorLogger.Error("Failed to register a consumer", "error", err)
		return nil, err
	}

	return msgs, nil
}
