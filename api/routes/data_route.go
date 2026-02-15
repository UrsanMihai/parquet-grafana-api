package routes

import (
	"github.com/gofiber/fiber/v3"
	"github.com/ursanmihai/parquet-grafana-api/api/handlers"
)

func dataRouter(app *fiber.App) {
	// Attache handlers.
	app.Get("/data/multiple/:columns/timestamp/:timestamp/format/:format", handlers.GetMultipleColumns())
	app.Get("/data/:column/timestamp/:timestamp/format/:format", handlers.GetSingleColumn())
}
