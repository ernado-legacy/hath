package main

import (
	"net/http"

	"github.com/GeertJohan/go.rice"
	"github.com/gin-gonic/gin"
)

const (
	boxStatic = "frontend"
)

var (
	box = rice.MustFindBox(boxStatic)
)

func static(c *gin.Context) {
	c.Request.URL.Path = c.Params.ByName("filepath")
	http.FileServer(box.HTTPBox()).ServeHTTP(c.Writer, c.Request)
}

func main() {
	e := gin.New()
	e.Use(gin.Logger())
	e.Use(gin.Recovery())
	e.GET("/gui/*filepath", static)
	e.Run(":1488")
}
