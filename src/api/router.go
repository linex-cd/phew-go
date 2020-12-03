package main

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

type Response struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data"`
}

func ResponseJson(c *gin.Context, code int, message string, data interface{}) {
	c.JSON(http.StatusOK, Response{
		Code:    code,
		Message: message,
		Data:    data,
	})
}

func LoadRouter(g *gin.Engine) *gin.Engine {

	//防止异常
	g.Use(gin.Recovery())

	// 404
	g.NoRoute(func(c *gin.Context) {
		c.String(http.StatusNotFound, "404 not found")
	})

	// 静态资源加载
	g.StaticFS("/monitor", http.Dir("../static"))
	//engine.StaticFile("/ping.txt", "./res/ping.txt")

	g.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/monitor/")
	})

	//state 路由
	g.GET("/state/system", system)
	g.GET("/state/latestwork", latestwork)

	return g
}
