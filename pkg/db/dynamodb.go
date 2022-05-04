package db

import (
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

type ConfigDynamoDB struct {
	Host string
	Port string
}

// New returns a new dynamodb connection.
func (c *ConfigDynamoDB) New() (*dynamodb.DynamoDB, error) {
	s, err := session.NewSession(aws.NewConfig().WithEndpoint(c.DataSourceName()))
	if err != nil {
		return nil, err
	}

	return dynamodb.New(s), nil
}

// SetupConfig populates dynamodb config using environment variables.
func (c *ConfigDynamoDB) SetupConfig() error {
	if c.Host = os.Getenv("DYNAMODB_SERVICE_HOST"); c.Host == "" {
		return errors.New("missing env DYNAMODB_SERVICE_HOST")
	}

	if c.Port = os.Getenv("DYNAMODB_SERVICE_PORT"); c.Port == "" {
		return errors.New("missing env DYNAMODB_SERVICE_PORT")
	}

	return nil
}

// DataSourceName returns a dynamodb session endpoint URL.
func (c *ConfigDynamoDB) DataSourceName() string {
	return fmt.Sprintf("http://%s:%s", c.Host, c.Port)
}
