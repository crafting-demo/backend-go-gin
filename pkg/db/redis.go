package db

import (
	"errors"
	"fmt"
	"os"

	"github.com/go-redis/redis/v8"
)

type ConfigRedis struct {
	Host string
	Port string
}

// New returns a new redis connection.
func (c *ConfigRedis) New() *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     c.DataSourceName(),
		Password: "",
		DB:       0, // default db
	})
}

// SetupConfig populates redis config using environment variables.
func (c *ConfigRedis) SetupConfig() error {
	if c.Host = os.Getenv("REDIS_SERVICE_HOST"); c.Host == "" {
		return errors.New("missing env REDIS_SERVICE_HOST")
	}

	if c.Port = os.Getenv("REDIS_SERVICE_PORT"); c.Port == "" {
		return errors.New("missing env REDIS_SERVICE_PORT")
	}

	return nil
}

// DataSourceName returns a redis HOST:PORT.
func (c *ConfigRedis) DataSourceName() string {
	return fmt.Sprintf("%s:%s", c.Host, c.Port)
}
