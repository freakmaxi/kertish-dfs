package data

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Connection struct {
	client      *mongo.Client
	transaction bool
}

func NewConnection(connectionString string, transaction bool) (*Connection, error) {
	client, err := mongo.NewClient(options.Client().ApplyURI(connectionString))
	if err != nil {
		return nil, err
	}
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*30)
	defer cancelFunc()

	err = client.Connect(ctx)
	if err != nil {
		return nil, err
	}

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		return nil, err
	}

	return &Connection{
		client:      client,
		transaction: transaction,
	}, nil
}
