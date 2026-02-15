package routes

import "github.com/gofiber/fiber/v3"

func AddRoutes(app *fiber.App) {
	// Attach routers.
	dataRouter(app)
}
