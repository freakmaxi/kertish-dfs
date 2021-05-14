package main

import (
	"encoding/json"
	"strings"

	"github.com/freakmaxi/kertish-dfs/basics/hooks"
	"github.com/streadway/amqp"
)

var version = "XX.X.XXXX"

func Name() string {
	return "RabbitMQ"
}

func Load() hooks.Action {
	return &RabbitMQ{}
}

// RabbitMQ struct is the Action provider for the action execution
type RabbitMQ struct {
	ConnectionUrl    string `json:"connectionUrl"`
	TargetQueueTopic string `json:"targetQueueTopic"`
}

func (r *RabbitMQ) Provider() string {
	return strings.ToLower(Name())
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
	return &RabbitMQ{}
}

func (r *RabbitMQ) Create(v json.RawMessage) error {
	return json.Unmarshal(v, r)
}

func (r *RabbitMQ) Serialize() json.RawMessage {
	b, err := json.Marshal(r)
	if err != nil {
		return nil
	}
	return b
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
