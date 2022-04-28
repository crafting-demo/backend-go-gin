package main

import (
	"log"
	"os"

	"github.com/crafting-demo/backend-go-gin/pkg/api"
)

func main() {
	var ctx api.Context

	ctx.Mode = "release"
	ctx.Port = os.Getenv("GIN_SERVICE_PORT")
	if ctx.Port == "" {
		log.Fatal("GIN_SERVICE_PORT must be set")
	}

	api.Run(ctx)
}
