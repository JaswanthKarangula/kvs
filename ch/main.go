package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"hash/fnv"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"time"
)

// srv struct handling server
var db = map[string]string{}

type srv struct {
	listenAddress string
	raft          *raft.Raft
	echo          *echo.Echo
}

// Start - start the server
func (s srv) Start() error {
	return s.echo.StartServer(&http.Server{
		Addr:         s.listenAddress,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
	})
}

// New return new server
func New(listenAddr string) *srv {
	e := echo.New()
	e.HideBanner = true
	e.HidePort = true
	e.Pre(middleware.RemoveTrailingSlash())
	e.GET("/debug/pprof/*", echo.WrapHandler(http.DefaultServeMux))

	e.POST("/store", Store)
	//e.GET("/store/:key", storeHandler.Get)
	//e.DELETE("/store/:key", storeHandler.Delete)

	return &srv{
		listenAddress: listenAddr,
		echo:          e,
	}
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

type requestStore struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func Store(eCtx echo.Context) error {
	var form = requestStore{}
	if err := eCtx.Bind(&form); err != nil {
		return eCtx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"error": fmt.Sprintf("error binding: %s", err.Error()),
		})
	}

	form.Key = strings.TrimSpace(form.Key)
	if form.Key == "" {
		return eCtx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"error": "key is empty",
		})
	}
	hashvalue := hash(form.Key)
	posturl := "http://localhost:2221/store"
	if hashvalue > 926844193 {
		posturl = "http://localhost:2225/store"
	}

	fmt.Println(posturl)
	db[form.Key] = form.Value
	values := map[string]string{"key": form.Key, "value": form.Value}

	jsonValue, _ := json.Marshal(values)

	_, err := http.Post(posturl, "application/json", bytes.NewBuffer(jsonValue))
	if err != nil {
		fmt.Println("Error \n", err)
	}
	return err
}

func Get(eCtx echo.Context) error {
	var key = strings.TrimSpace(eCtx.Param("key"))
	if key == "" {
		return eCtx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"error": "key is empty",
		})
	}

	hashvalue := hash(key)
	geturl := "http://localhost:2221/store"
	if hashvalue > 926844193 {
		geturl = "http://localhost:2225/store"
	}
	geturl = geturl + "/" + key

	fmt.Println(geturl)

	_, err := http.Get(geturl)

	if err != nil {
		fmt.Println("Error \n", err)
	}

	if db[key] == "" {
		return eCtx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"error": fmt.Sprintf("error getting key %s from storage: %s", key, "Key not found"),
		})
	}

	return eCtx.JSON(http.StatusOK, map[string]interface{}{
		"message": "success fetching data",
		"data": map[string]interface{}{
			"key":   key,
			"value": db[key],
		},
	})

}

func Delete(eCtx echo.Context) error {
	var key = strings.TrimSpace(eCtx.Param("key"))
	if key == "" {
		return eCtx.JSON(http.StatusUnprocessableEntity, map[string]interface{}{
			"error": "key is empty",
		})
	}

	hashvalue := hash(key)
	posturl := "http://localhost:2221/store"
	if hashvalue > 926844193 {
		posturl = "http://localhost:2225/store"
	}

	fmt.Println(posturl)

	db[key] = ""

	//_, err := http.
	//if err != nil {
	//	fmt.Println("Error \n", err)
	//	return err
	//}

	return eCtx.JSON(http.StatusOK, map[string]interface{}{
		"message": "success removing data",
		"data": map[string]interface{}{
			"key":   key,
			"value": nil,
		},
	})
}

func main() {
	e := echo.New()

	// Middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Routes
	e.POST("/store", Store)
	e.GET("/store/:key", Get)
	e.DELETE("/store/:key", Delete)
	e.Logger.Fatal(e.Start(":8083"))

}
