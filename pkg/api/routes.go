package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// A routing model.
type Route struct {
	Method   string
	Endpoint string
	Handler  gin.HandlerFunc
}

// A collection of routes.
type Routes []Route

var routes = Routes{
	Route{http.MethodPost, "/", Demo},
}
