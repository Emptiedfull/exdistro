package main

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/gofiber/fiber/v2"
)

type MassSet map[string][]byte
type MassGet []string
type MassGetRes map[string][]byte

func startClientApi(node *Node) {
	app := fiber.New()

	app.Delete("/api/:key", func(c *fiber.Ctx) error {
		err := node.delClientOne(c.Params("key"))
		if err != nil {
			c.SendString(err.Error())
		}
		return c.SendString("SUCCESS")
	})

	app.Get("/api/massget", func(c *fiber.Ctx) error {
		var massGet MassGet
		if err := json.Unmarshal(c.Body(), &massGet); err != nil {
			fmt.Println(string(c.Body()))
			return c.SendStatus(fiber.StatusNotAcceptable)
		}

		obj, err := node.getClientMass(massGet)
		fmt.Println(obj)
		if err != nil {
			c.SendString("error")
		}
		return c.JSON(obj)

	})

	app.Get("/api/:key", func(c *fiber.Ctx) error {
		res, err := node.getClientOne(c.Params("key"))
		if err != nil {
			return c.SendString(err.Error())
		}

		return c.SendString(string(res))
	})

	app.Post("/api/:key/:value", func(c *fiber.Ctx) error {
		key := c.Params("key")
		val := c.Params("value")
		err := node.setClientOne(key, []byte(val))
		if err != nil {
			return c.SendString(err.Error())
		}
		return c.SendString("SUCCESS")
	})

	app.Post("/api/massset", func(c *fiber.Ctx) error {

		var massSet MassSet
		if err := json.Unmarshal(c.Body(), &massSet); err != nil {
			fmt.Println(c.Body())
			return c.SendStatus(fiber.StatusNotAcceptable)
		}

		fmt.Println(string(massSet["haha"]))

		err := node.setClientMass(massSet)
		if err != nil {
			c.SendStatus(400)
		}

		return c.SendString("SUCCESS")
	})

	port := Config.port + 1
	app.Listen(":" + strconv.Itoa(port))
}
