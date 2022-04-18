package db

import (
	"database/sql"
	"errors"
	"fmt"
	"os"

	_ "github.com/lib/pq"
)

type ConfigPostgres struct {
	// User
	User string

	// Password
	Pass string

	// Database
	DB string

	// Host
	Host string

	// Port
	Port string
}

// New returns a new postgres connection.
func (c *ConfigPostgres) New() (*sql.DB, error) {
	if err := c.SetupConfig(); err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", c.DataSourceName())
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// SetupConfig populates postgres config using environment variables.
func (c *ConfigPostgres) SetupConfig() error {
	// set user
	c.User = DBUser

	// set password
	c.Pass = DBPass

	// set database
	c.DB = DBName

	// set host
	if c.Host = os.Getenv("POSTGRES_SERVICE_HOST"); c.Host == "" {
		return errors.New("missing env POSTGRES_SERVICE_HOST")
	}

	// set port
	if c.Port = os.Getenv("POSTGRES_SERVICE_PORT"); c.Port == "" {
		return errors.New("missing env POSTGRES_SERVICE_PORT")
	}

	return nil
}

// DataSourceName returns a connection string suitable for sql.Open.
func (c *ConfigPostgres) DataSourceName() string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		c.Host, c.Port, c.User, c.Pass, c.DB)
}
