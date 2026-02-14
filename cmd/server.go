package cmd

import (
	"database/sql"
	"fmt"
	"strconv"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/log"
	"github.com/spf13/cobra"
)

type BasicQueryResult struct {
	Time   int64   `json:"time"`
	Number float32 `json:"number"`
}

func exec(cmd *cobra.Command, args []string) {
	// Check the CLI flags.
	parquet_path, _ := cmd.Flags().GetString("parquet_path")
	port, _ := cmd.Flags().GetString("port")
	if parquet_path == "" {
		log.Error("Please provide the path to the Parquet file using --parquet_path")
		return
	}
	if port == "" {
		log.Error("Please provide the port of the server using --port port_number")
		return
	}

	// Initialize the Fiber app.
	app := fiber.New()

	app.Get("/data/:column", func(c fiber.Ctx) error {
		log.Infof(c.OriginalURL())
		column := c.Params("column")
		fromMs, err := strconv.ParseInt(c.Query("from"), 10, 64)
		if err != nil {
			log.Errorf("Bad query param format, from timestamp! 0 will be used as a default value for from query param.")
			fromMs = 0
		}
		toMs, err := strconv.ParseInt(c.Query("to"), 10, 64)
		if err != nil {
			log.Errorf("Bad query param format, to timestamp! Current timestamp will be used as a default value for to query param.")
			toMs = time.Now().UTC().UnixMilli()
		}
		deviceAlias := c.Query("device_alias")
		// Try to open a connection to DuckDB.
		db, err := sql.Open("duckdb", "")
		if err != nil {
			log.Errorf("Failed to connect to DuckDB: %v", err)
			return fiber.NewError(fiber.ErrInternalServerError.Code, "Failed to open the query engine!")
		}
		defer db.Close()

		// Execute the query to read from the Parquet file and filter based on the provided parameters.
		rows, err := db.Query(fmt.Sprintf("SELECT %s, CAST(rcv_timestamp / 1e6 as BIGINT) as time FROM read_parquet('%s') WHERE time BETWEEN %d AND %d AND device_alias = '%s';", column, parquet_path, fromMs, toMs, deviceAlias))
		if err != nil {
			log.Errorf("Failed to execute query: %v", err)
			return fiber.NewError(fiber.ErrInternalServerError.Code, "Failed to execute query!")
		}
		defer rows.Close()

		var columnValues []BasicQueryResult
		for rows.Next() {
			var columnValue BasicQueryResult

			if err := rows.Scan(&columnValue.Number, &columnValue.Time); err != nil {
				log.Errorf("Failed to scan row: %v", err)
				return fiber.NewError(fiber.ErrInternalServerError.Code, "Error while parsing the data!")
			}
			columnValues = append(columnValues, columnValue)
		}
		return c.JSON(columnValues)
	})

	app.Listen(":" + port)
}

var cmd = &cobra.Command{
	Use:   "server",
	Short: "Main server command to launch the grafana parquet plugin.",
	Run:   exec,
}

func Execute() {
	cmd.Flags().StringP("parquet_path", "f", "", "Path to the Parquet file")
	cmd.Flags().StringP("port", "p", "3000", "Port to run the server on")
	if err := cmd.Execute(); err != nil {
		log.Errorf("Failed to execute command: %v", err)
	}
}
