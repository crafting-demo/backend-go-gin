package db

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type ConfigMongoDB struct {
	Host string
	Port string
}

// New returns a new mongo connection.
func (c *ConfigMongoDB) New() (*mongo.Client, context.Context, context.CancelFunc, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	opts := options.Client().ApplyURI(c.DataSourceName())
	client, err := mongo.NewClient(opts)
	if err != nil {
		return nil, ctx, cancel, err
	}

	if err := client.Connect(ctx); err != nil {
		return nil, ctx, cancel, err
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		return nil, ctx, cancel, err
	}

	return client, ctx, cancel, nil
}

// SetupConfig populates mongo config using environment variables.
func (c *ConfigMongoDB) SetupConfig() error {
	if c.Host = os.Getenv("MONGODB_SERVICE_HOST"); c.Host == "" {
		return errors.New("missing env MONGODB_SERVICE_HOST")
	}

	if c.Port = os.Getenv("MONGODB_SERVICE_PORT"); c.Port == "" {
		return errors.New("missing env MONGODB_SERVICE_PORT")
	}

	return nil
}

// DataSourceName returns a mongo connection URI.
func (c *ConfigMongoDB) DataSourceName() string {
	return fmt.Sprintf("mongodb://%s:%s", c.Host, c.Port)
}
