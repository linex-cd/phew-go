package api

import "net/http"
import "github.com/gin-gonic/gin"

type Response struct {
	Code int `json:"code"`
	Message string `json:"message"`
	Data interface{} `json:"data"`
}

func Response(c *gin.Context, code int, message string, data interface{}) {
    c.JSON(http.StatusOK, Response{
        Code: code,
        Message: message,
        Data: data,
    })
}


func LoadRouter(g *gin.Engine) *gin.Engine {

	//防止异常
	g.Use(gin.Recovery())
	
    // 404
    g.NoRoute(func (c *gin.Context)  {
        c.String(http.StatusNotFound, "404 not found");
    })

	//state 路由
    g.GET("/test", test)



    return g
}