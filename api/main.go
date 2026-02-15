package api

import (
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/log"
	"github.com/ursanmihai/parquet-grafana-api/api/database"
	"github.com/ursanmihai/parquet-grafana-api/api/routes"
	apiTypes "github.com/ursanmihai/parquet-grafana-api/api/types"
)

func Main(config apiTypes.APIConfig) {
	app := fiber.New()

	repo, err := database.Init("duckdb", config.DataSource)
	if err != nil {
		log.Error("Error while oppening database driver!")
		return
	}
	defer repo.Close()

	routes.AddRoutes(app)
	app.Listen(":" + config.Port)
}
