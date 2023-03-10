package api

import (
	"encoding/json"
	"os"
	"os/signal"

	"github.com/Shopify/sarama"
	"github.com/crafting-demo/backend-go-gin/pkg/kafka"
	"github.com/crafting-demo/backend-go-gin/pkg/logger"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

type Context struct {
	Mode string
	Port string
}

func GinRun(ctx Context) {
	gin.SetMode(ctx.Mode)

	router := gin.New()

	router.Use(gin.Recovery())
	router.Use(cors.Default())

	router.POST("/api", HttpHandler)
	router.NoRoute(BadRequest)

	router.Run(":" + ctx.Port)
}

func KafkaRun() {
	var consumer kafka.Consumer

	conn, err := consumer.New()
	if err != nil {
		logger.Write("Connecting to kafka...")
		go KafkaRun()
		return
	}
	defer conn.Close()

	logger.Write("Connection established. Ready for requests!\n\n")

	partitionConsumer, err := conn.ConsumePartition("backend-go-gin", 0, sarama.OffsetNewest)
	if err != nil {
		logger.Writef("KafkaRun", "failed to create partition consumer", err)
	}
	defer partitionConsumer.Close()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			var message RequestMessage
			if err := json.Unmarshal(msg.Value, &message); err != nil {
				logger.Writef("KafkaRun", "failed to parse json encoded message", err)
				continue
			}
			go KafkaHandler(message)
		case <-signals:
			return
		}
	}
}
