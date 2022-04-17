package api

import (
	"encoding/json"

	"github.com/crafting-demo/backend-go-gin/pkg/queue"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

type Context struct {
	// Gin mode
	Mode string

	// Server port
	Port string
}

func Init(ctx Context) {
	// Set gin mode
	gin.SetMode(ctx.Mode)

	// Gin router with default middleware:
	// logger and recovery (crash-free) middleware
	router := gin.Default()

	// Same as
	// config := cors.DefaultConfig()
	// config.AllowAllOrigins = true
	// router.Use(cors.New(config))
	router.Use(cors.Default())

	// Handle api routes
	for _, route := range routes {
		router.Handle(route.Method, route.Endpoint, route.Handler)
	}

	// If no routers match request URL,
	// return 400 (Bad Request)
	router.NoRoute(BadRequest)

	// Listen and serve on 0.0.0.0:PORT
	router.Run(":" + ctx.Port)

	var listener queue.Consumer
	msgCh := make(chan []byte)
	go listener.Run(queue.GoGin, msgCh)
	for {
		m := <-msgCh
		if len(m) > 0 {
			var msg queue.Message
			json.Unmarshal(m, &msg)
			HandleMessage(msg)
		}
	}
}
