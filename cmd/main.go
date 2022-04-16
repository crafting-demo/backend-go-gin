package main

import "github.com/crafting-demo/backend-go-gin/pkg/api"

func main() {
	var ctx api.Context

	ctx.Mode = "release"
	ctx.Port = "8080"

	api.Init(ctx)
}
