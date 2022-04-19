package api

import (
	"context"
	"database/sql"
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

func mysqlRead(key string) (string, error) {
	var config db.ConfigMySQL
	if err := config.SetupConfig(); err != nil {
		return "", err
	}

	conn, err := config.New()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	query := fmt.Sprintf("select content from %s where uuid = '%s'", db.DBName, key)
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

func mysqlWrite(key string, value string) error {
	var config db.ConfigMySQL
	if err := config.SetupConfig(); err != nil {
		return err
	}

	conn, err := config.New()
	if err != nil {
		return err
	}
	defer conn.Close()

	stmt := fmt.Sprintf("insert into %s (uuid, content) values ($1, $2) returning uuid", db.DBName)
	row := conn.QueryRow(stmt, key, value)

	var uuid string
	if err := row.Scan(&uuid); err != nil {
		return err
	}

	return nil
}

func postgresRead(key string) (string, error) {
	var config db.ConfigPostgres
	if err := config.SetupConfig(); err != nil {
		return "", err
	}

	conn, err := config.New()
	if err != nil {
		return "", err
	}
	defer conn.Close()

	query := fmt.Sprintf("select content from %s where uuid = '%s'", db.DBName, key)
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

func postgresWrite(key string, value string) error {
	var config db.ConfigPostgres
	if err := config.SetupConfig(); err != nil {
		return err
	}

	conn, err := config.New()
	if err != nil {
		return err
	}
	defer conn.Close()

	stmt := fmt.Sprintf("insert into %s (uuid, content) values ($1, $2) returning uuid", db.DBName)
	row := conn.QueryRow(stmt, key, value)

	var uuid string
	if err := row.Scan(&uuid); err != nil {
		return err
	}

	return nil
}

func mongoRead(key string) (string, error) {
	var config db.ConfigMongo
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

func mongoWrite(key string, value string) error {
	var config db.ConfigMongo
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

func dynamoRead(key string) (string, error) {
	var config db.ConfigDynamo
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

func dynamoWrite(key string, value string) error {
	var config db.ConfigDynamo
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

func redisRead(key string) (string, error) {
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

func redisWrite(key string, value string) error {
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
