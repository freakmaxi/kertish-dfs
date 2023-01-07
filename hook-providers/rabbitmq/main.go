package main

import (
	"encoding/json"

	"github.com/freakmaxi/kertish-dfs/basics/hooks"
	"github.com/streadway/amqp"
)

var version = "XX.X.XXXX"

func Load() hooks.Action {
	return &RabbitMQ{}
}

// RabbitMQ struct is the Action provider for the action execution
type RabbitMQ struct {
	ConnectionUrl    string `json:"connectionUrl"`
	TargetQueueTopic string `json:"targetQueueTopic"`
}

func (r *RabbitMQ) Provider() string {
	return "rabbitmq"
}

func (r *RabbitMQ) Version() string {
	return version
}

func (r *RabbitMQ) Sample() interface{} {
	return &RabbitMQ{
		ConnectionUrl:    "amqp://test:test@127.0.0.1:5672/",
		TargetQueueTopic: "testQueueName",
	}
}

func (r *RabbitMQ) New() hooks.Action {
	return Load()
}

func (r *RabbitMQ) Setup(v hooks.SetupMap) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return json.Unmarshal(b, r)
}

func (r *RabbitMQ) Execute(aI *hooks.ActionInfo) error {
	buf, err := json.Marshal(aI)
	if err != nil {
		return err
	}

	conn, err := amqp.Dial(r.ConnectionUrl)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	defer func() { _ = channel.Close() }()

	if _, err := channel.QueueDeclare(r.TargetQueueTopic, true, false, false, false, nil); err != nil {
		return err
	}

	if err := channel.Publish(
		"",
		r.TargetQueueTopic,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        buf,
		},
	); err != nil {
		return err
	}

	return nil
}

var _ hooks.Action = &RabbitMQ{}
