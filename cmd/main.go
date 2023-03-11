package main

import (
	"os"

	"github.com/crafting-demo/backend-go-gin/pkg/api"
)

func main() {
	var ctx api.Context

	ctx.Mode = "release"
	ctx.Port = os.Getenv("GIN_SERVICE_PORT")
	if ctx.Port == "" {
		ctx.Port = "8081"
	}

	go api.KafkaRun()
	api.GinRun(ctx)
}
