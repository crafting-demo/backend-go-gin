package api

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/crafting-demo/backend-go-gin/pkg/db"
)

// ReadEntity returns value of key from some data store.
func ReadEntity(store string, key string) (string, error) {
	var value string
	var err error
	switch store {
	case db.MySQL:
		value, err = readMySQL(key)
	default:
		return value, errors.New("unsupported data store: " + store)
	}
	return value, err
}

// WriteEntity writes a key/value pair to some data store.
func WriteEntity(store string, key string, value string) error {
	var err error
	switch store {
	case db.MySQL:
		err = writeMySQL(key, value)
	default:
		return errors.New("unsupported data store: " + store)
	}
	return err
}

// readMySQL returns value of key from mysql.
func readMySQL(key string) (string, error) {
	var config db.ConfigMySQL
	if err := config.SetupConfig(); err != nil {
		return "", err
	}

	conn, err := config.New()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	query := fmt.Sprintf("select content from %s where uuid = '%s'", db.DBCollection, key)
	row := conn.QueryRow(query)

	var value string
	if err := row.Scan(&value); err != nil {
		if err != sql.ErrNoRows {
			return "", err
		}
		value = "Not Found"
	}

	return value, nil
}

// writeMySQL stores key/value pair in mysql.
func writeMySQL(key string, value string) error {
	var config db.ConfigMySQL
	if err := config.SetupConfig(); err != nil {
		return err
	}

	conn, err := config.New()
	if err != nil {
		return err
	}
	defer conn.Close()

	stmt := fmt.Sprintf("insert into %s (uuid, content) values (?, ?) ON DUPLICATE KEY UPDATE content = ?", db.DBCollection)
	row, err := conn.Exec(stmt, key, value, value)
	if err != nil {
		return err
	}

	_, err = row.LastInsertId()
	if err != nil {
		return err
	}

	return nil
}
