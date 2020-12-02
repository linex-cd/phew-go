package main

import "net/http"
import "github.com/gin-gonic/gin"
import "github.com/shirou/gopsutil"

import "./api"

func main() {
	engine := gin.Default()
	// 静态资源加载
	engine.StaticFS("/", http.Dir("./static"))
	//router.StaticFile("/ping.txt", "./res/ping.txt")
	
	engine.Use(gin.Recovery())
    // 404
    engine.NoRoute(func (c *gin.Context)  {
        c.String(http.StatusNotFound, "404 not found");
    })

	api.LoadRouter(engine)
	
	// Listen and serve on 0.0.0.0:80
	router.Run("0.0.0.0:2020")
}