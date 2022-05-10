package api

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/crafting-demo/backend-go-gin/pkg/db"
	"github.com/go-redis/redis/v8"
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
	case db.Postgres:
		value, err = readPostgres(key)
	case db.MongoDB:
		value, err = readMongoDB(key)
	case db.DynamoDB:
		value, err = readDynamoDB(key)
	case db.Redis:
		value, err = readRedis(key)
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
	case db.Postgres:
		err = writePostgres(key, value)
	case db.MongoDB:
		err = writeMongoDB(key, value)
	case db.DynamoDB:
		err = writeDynamoDB(key, value)
	case db.Redis:
		err = writeRedis(key, value)
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

// readPostgres returns value of key from postgres.
func readPostgres(key string) (string, error) {
	var config db.ConfigPostgres
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

// writePostgres stores key/value pair in postgres.
func writePostgres(key string, value string) error {
	var config db.ConfigPostgres
	if err := config.SetupConfig(); err != nil {
		return err
	}

	conn, err := config.New()
	if err != nil {
		return err
	}
	defer conn.Close()

	stmt := fmt.Sprintf("insert into %s (uuid, content) values ($1, $2) returning uuid", db.DBCollection)
	row := conn.QueryRow(stmt, key, value)

	var uuid string
	if err := row.Scan(&uuid); err != nil {
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

// readDynamoDB returns value of key from dynamodb.
func readDynamoDB(key string) (string, error) {
	var config db.ConfigDynamoDB
	if err := config.SetupConfig(); err != nil {
		return "", err
	}

	conn, err := config.New()
	if err != nil {
		return "", err
	}

	filter := &dynamodb.GetItemInput{
		TableName: aws.String(db.DBCollection),
		Key: map[string]*dynamodb.AttributeValue{
			"uuid": {
				S: aws.String(key),
			},
		},
	}

	result, err := conn.GetItem(filter)
	if result.Item == nil {
		return "Not Found", nil
	}
	if err != nil {
		return "", err
	}

	var item struct {
		Uuid    string `bson:"uuid"`
		Content string `bson:"content"`
	}

	err = dynamodbattribute.UnmarshalMap(result.Item, &item)
	if err != nil {
		return "", err
	}

	return item.Content, nil
}

// writeDynamoDB stores key/value pair in dynamodb.
func writeDynamoDB(key string, value string) error {
	var config db.ConfigDynamoDB
	if err := config.SetupConfig(); err != nil {
		return err
	}

	conn, err := config.New()
	if err != nil {
		return err
	}

	type item struct {
		Uuid    string `bson:"uuid"`
		Content string `bson:"content"`
	}
	av, err := dynamodbattribute.MarshalMap(item{Uuid: key, Content: value})
	if err != nil {
		return err
	}

	input := &dynamodb.PutItemInput{
		Item:      av,
		TableName: aws.String(db.DBCollection),
	}
	_, err = conn.PutItem(input)
	if err != nil {
		return err
	}

	return nil
}

// readRedis returns value of key from redis.
func readRedis(key string) (string, error) {
	var config db.ConfigRedis
	if err := config.SetupConfig(); err != nil {
		return "", err
	}

	conn := config.New()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	value, err := conn.Get(ctx, key).Result()
	if err == redis.Nil {
		return "Not Found", nil
	}
	if err != nil {
		return "", err
	}

	return value, nil
}

// writeRedis stores key/value pair in redis.
func writeRedis(key string, value string) error {
	var config db.ConfigRedis
	if err := config.SetupConfig(); err != nil {
		return err
	}

	conn := config.New()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := conn.Set(ctx, key, value, 0).Err(); err != nil {
		return err
	}

	return nil
}
