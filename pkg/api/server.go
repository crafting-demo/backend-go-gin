package api

import (
	"github.com/gin-gonic/gin"
)

type Context struct {
	Mode string
	Port string
}

func Run(ctx Context) {
	gin.SetMode(ctx.Mode)

	router := gin.Default()

	router.POST("/", NestedCallHandler)
	router.NoRoute(BadRequest)

	router.Run(":" + ctx.Port)
}
