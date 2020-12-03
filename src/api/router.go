package main

import (
	"net/http"
	"path"

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

func ResponseFile(c *gin.Context, filepath string) {

	fileName := path.Base(filepath)
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Disposition", "attachment; filename="+fileName)
	c.Header("Content-Transfer-Encoding", "binary")
	c.Header("Cache-Control", "no-cache")

	if Existfile(filepath) == false {
		ResponseJson(c, 404, "file not found", make(map[string]string))
		return
	}
	c.Header("Content-Type", "application/octet-stream")
	c.Header("Content-Disposition", "attachment; filename="+fileName)
	c.Header("Content-Transfer-Encoding", "binary")

	c.File(filepath)
	return
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
	g.GET("/state/jobcounter", jobcounter)
	g.GET("/state/nodecounter", nodecounter)
	g.GET("/state/peekjob", peekjob)
	g.GET("/state/peektask", peektask)
	g.GET("/state/peekfile", peekfile)

	return g
}
