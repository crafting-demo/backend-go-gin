package db

import (
	"database/sql"
	"fmt"

	// go mysql driver
	_ "github.com/go-sql-driver/mysql"
)

type ConfigMySQL struct {
	User string
	Pass string
	DB   string
	Host string
	Port string
}

// New returns a new mysql connection.
func (c *ConfigMySQL) New() (*sql.DB, error) {
	if err := c.SetupConfig(); err != nil {
		return nil, err
	}

	db, err := sql.Open("mysql", c.DataSourceName())
	if err != nil {
		return nil, err
	}

	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}

	return db, nil
}

// SetupConfig populates mysql config using environment variables.
func (c *ConfigMySQL) SetupConfig() error {
	c.DB = DBName

	c.User = DBUser
	c.Pass = DBPass
	c.Host = "localhost"
	c.Port = "3306"

	// if c.Host = os.Getenv("MYSQL_SERVICE_HOST"); c.Host == "" {
	// 	return errors.New("missing env MYSQL_SERVICE_HOST")
	// }

	// if c.Port = os.Getenv("MYSQL_SERVICE_PORT"); c.Port == "" {
	// 	return errors.New("missing env MYSQL_SERVICE_PORT")
	// }

	return nil
}

// DataSourceName returns a connection string suitable for sql.Open.
func (c *ConfigMySQL) DataSourceName() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", c.User, c.Pass, c.Host, c.Port, c.DB)
}
