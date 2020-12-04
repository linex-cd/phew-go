package main

import "net/http"
import "github.com/gin-gonic/gin"

//build
//go run config.go util.go state.go router.go main.go

func main() {
	engine := gin.Default()
	
	
	engine.Use(gin.Recovery())
    // 404
    engine.NoRoute(func (c *gin.Context)  {
        c.String(http.StatusNotFound, "404 not found");
    })

	LoadRouter(engine)

	
	
	// Listen and serve on 0.0.0.0:80
	engine.Run("0.0.0.0:2020")
}