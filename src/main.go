package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

func main() {

	go health_thread(300)
	go daemon_thread()

	engine := gin.Default()

	engine.Use(gin.Recovery())
	// 404
	engine.NoRoute(func(c *gin.Context) {
		c.String(http.StatusNotFound, "404 not found")
	})

	LoadRouter(engine)

	// Listen and serve on 0.0.0.0:80
	engine.Run("0.0.0.0:2020")
}
