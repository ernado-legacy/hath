package main

import (
	"net/http"

	"cydev.ru/hath"

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

func stats(c *gin.Context) {
	stats := hath.Stats{}
	stats.FilesDownloaded = 1024
	stats.FilesSent = 512
	stats.FilesDownloadedBytes = 1024 * 1024 * 50
	stats.FilesSentBytes = 1024 * 1024 * 25
	c.JSON(http.StatusOK, stats)
}

func main() {
	e := gin.New()
	e.Use(gin.Logger())
	e.Use(gin.Recovery())
	e.GET("/gui/*filepath", static)
	e.GET("/api/stats", stats)
	e.Run(":1488")
}
