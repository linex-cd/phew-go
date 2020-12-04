package main

import (
	"net/http"
	"path"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis"
)

var r *redis.Client = nil

func getRedisInstance() *redis.Client {
	if r == nil {
		r = redis.NewClient(&redis.Options{
			Addr:     Redis_host + ":2019",
			Password: "",
			DB:       0,
		})

	}

	return r
}

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
	g.StaticFS("/monitor", http.Dir("./static"))
	//engine.StaticFile("/ping.txt", "./res/ping.txt")

	g.GET("/", func(c *gin.Context) {
		c.Redirect(http.StatusMovedPermanently, "/monitor/")
	})

	g.GET("/hello", func(c *gin.Context) {

		name := c.DefaultQuery("name", "")
		ResponseJson(c, 200, "ok", name)

	})

	//state 路由
	g.GET("/state/system", system)
	g.GET("/state/latestwork", latestwork)
	g.GET("/state/jobcounter", jobcounter)
	g.GET("/state/nodecounter", nodecounter)
	g.GET("/state/peekjob", peekjob)
	g.GET("/state/peektask", peektask)
	g.GET("/state/peekfile", peekfile)
	g.GET("/state/percentage", percentage)
	g.GET("/state/errorlist", errorlist)

	//task 路由
	g.POST("/task/ping", taskping)
	g.POST("/task/get", get)
	g.POST("/task/finish", finish)

	//job 路由
	g.POST("/job/ping", jobping)
	g.POST("/job/assign", assign)
	g.POST("/job/delete", delete)
	g.POST("/job/done", done)
	g.POST("/job/detail", detail)
	g.POST("/job/read", read)
	g.POST("/job/retry", retry)

	return g
}
