package api

import (
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

type Context struct {
	Mode string
	Port string
}

func Run(ctx Context) {
	gin.SetMode(ctx.Mode)

	router := gin.New()

	router.Use(gin.Recovery())
	router.Use(cors.Default())

	router.POST("/api", NestedCallHandler)
	router.NoRoute(BadRequest)

	router.Run(":" + ctx.Port)
}
