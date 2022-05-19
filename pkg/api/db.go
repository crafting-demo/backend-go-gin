package api

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/crafting-demo/backend-go-gin/pkg/db"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// ReadEntity returns value of key from some data store.
func ReadEntity(store string, key string) (string, error) {
	var value string
	var err error
	switch store {
	case db.MySQL:
		value, err = readMySQL(key)
	case db.MongoDB:
		value, err = readMongoDB(key)
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
	case db.MongoDB:
		err = writeMongoDB(key, value)
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

	stmt := fmt.Sprintf("insert into %s (uuid, content) values (?, ?)", db.DBCollection)
	row, err := conn.Exec(stmt, key, value)
	if err != nil {
		return err
	}

	_, err = row.LastInsertId()
	if err != nil {
		return err
	}

	return nil
}

// readMongoDB returns value of key from mongodb.
func readMongoDB(key string) (string, error) {
	var config db.ConfigMongoDB
	if err := config.SetupConfig(); err != nil {
		return "", err
	}

	client, ctx, cancel, err := config.New()
	if err != nil {
		return "", err
	}
	defer cancel()
	defer client.Disconnect(ctx)

	collection := client.Database(db.DBName).Collection(db.DBCollection)

	var result struct {
		Uuid    string `bson:"uuid"`
		Content string `bson:"content"`
	}
	filter := bson.D{{Key: "uuid", Value: key}}

	err = collection.FindOne(ctx, filter).Decode(&result)
	if err == mongo.ErrNoDocuments {
		return "Not Found", nil
	}
	if err != nil {
		return "", err
	}

	return result.Content, nil
}

// writeMongoDB stores key/value pair in mongodb.
func writeMongoDB(key string, value string) error {
	var config db.ConfigMongoDB
	if err := config.SetupConfig(); err != nil {
		return err
	}

	client, ctx, cancel, err := config.New()
	if err != nil {
		return err
	}
	defer cancel()
	defer client.Disconnect(ctx)

	collection := client.Database(db.DBName).Collection(db.DBCollection)

	_, err = collection.InsertOne(ctx, bson.D{{Key: "uuid", Value: key}, {Key: "content", Value: value}})
	if err != nil {
		return err
	}

	return nil
}
