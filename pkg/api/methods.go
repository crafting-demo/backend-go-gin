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
	"github.com/crafting-demo/backend-go-gin/pkg/queue"
	"github.com/go-redis/redis/v8"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

// HandleMessage processes a message action.
func HandleMessage(msg queue.Message) {
	for _, action := range msg.Actions {
		switch action.Action {
		case queue.Echo:
			go Echo(action, msg.Meta)
		case queue.Read:
			go Read(action, msg.Meta)
		case queue.Write:
			go Write(action, msg.Meta)
		case queue.Call:
			go Call(action, msg.Meta)
		}
	}
}

// Echo processes Echo action on message.
func Echo(action queue.Action, meta queue.Meta) {
	msg := queue.Message{
		Meta: queue.Meta{
			Caller:   meta.Callee,
			Callee:   queue.TsReact,
			CallTime: queue.Timestamp(time.Now()),
		},
	}
	msg.Actions = append(msg.Actions, action)

	var broker queue.Producer
	broker.Enqueue(queue.TsReact, msg)
}

// Read processes Read action on message.
func Read(action queue.Action, meta queue.Meta) {
	msg := queue.Message{
		Meta: queue.Meta{
			Caller:   meta.Callee,
			Callee:   queue.TsReact,
			CallTime: queue.Timestamp(time.Now()),
		},
	}

	key := action.Payload.Key
	var value string
	var err error
	switch action.Payload.ServiceName {
	case db.MySQL:
		value, err = mysqlRead(key)
	case db.Postgres:
		value, err = postgresRead(key)
	case db.MongoDB:
		value, err = mongoRead(key)
	case db.DynamoDB:
		value, err = dynamoRead(key)
	case db.Redis:
		value, err = redisRead(key)
	}

	if err != nil {
		action.Payload.Value = fmt.Sprint(err)
	} else {
		action.Payload.Value = value
	}

	msg.Actions = append(msg.Actions, action)

	var broker queue.Producer
	broker.Enqueue(queue.TsReact, msg)
}

// Write processes Write action on message.
func Write(action queue.Action, meta queue.Meta) {
	msg := queue.Message{
		Meta: queue.Meta{
			Caller:   meta.Callee,
			Callee:   queue.TsReact,
			CallTime: queue.Timestamp(time.Now()),
		},
	}

	key, value := action.Payload.Key, action.Payload.Value
	var err error
	switch action.Payload.ServiceName {
	case db.MySQL:
		err = mysqlWrite(key, value)
	case db.Postgres:
		err = postgresWrite(key, value)
	case db.MongoDB:
		err = mongoWrite(key, value)
	case db.DynamoDB:
		err = dynamoWrite(key, value)
	case db.Redis:
		err = redisWrite(key, value)
	}

	if err != nil {
		action.Payload.Value = fmt.Sprint(err)
	}

	msg.Actions = append(msg.Actions, action)

	var broker queue.Producer
	broker.Enqueue(queue.TsReact, msg)
}

// Call processes Call action on message.
func Call(action queue.Action, meta queue.Meta) {
	msg := queue.Message{
		Meta: queue.Meta{
			Caller:   meta.Callee,
			Callee:   action.Payload.ServiceName,
			CallTime: queue.Timestamp(time.Now()),
		},
		Actions: action.Payload.Actions,
	}

	var broker queue.Producer
	broker.Enqueue(action.Payload.ServiceName, msg)
}

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

	collection := client.Database(db.DBName).Collection(db.Table)

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

	collection := client.Database(db.DBName).Collection(db.Table)

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
		TableName: aws.String(db.Table),
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
		TableName: aws.String(db.Table),
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
