package handlers

import (
	"fmt"
	"strconv"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/log"
	"github.com/ursanmihai/parquet-grafana-api/api/database"
	apiTypes "github.com/ursanmihai/parquet-grafana-api/api/types"
	"github.com/ursanmihai/parquet-grafana-api/api/utils"
)

func GetMultipleColumns() fiber.Handler {
	return func(c fiber.Ctx) error {
		log.Infof(c.OriginalURL())
		column := c.Params("columns")
		timestamp_column := c.Params("timestamp")
		timestamp_format := c.Params("format")

		if timestamp_column == "" {
			log.Error("A timestamp column must be given!")
			return fiber.NewError(fiber.ErrInternalServerError.Code, "A timestamp column must be given!")
		}
		ts_factor := utils.ToMS(timestamp_format)

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
		// Get the singleton instance of the DB driver connection.
		repository := database.GetInstance()
		db := repository.DB
		// Execute the query to read from the Parquet file and filter based on the provided parameters.
		rows, err := db.Query(fmt.Sprintf("SELECT %s, CAST(%s * %E as UBIGINT) as time FROM read_parquet('%s') WHERE time BETWEEN %d AND %d AND device_alias = '%s';", column, timestamp_column, ts_factor, repository.DataSource, fromMs, toMs, deviceAlias))
		if err != nil {
			log.Errorf("Failed to execute query: %v", err)
			return fiber.NewError(fiber.ErrInternalServerError.Code, "Failed to execute query!")
		}
		defer rows.Close()
		result_set_columns, _ := rows.Columns()
		numberOfColumns := len(result_set_columns)
		var columnValues []map[string]interface{}

		for rows.Next() {
			var columnsDataPtrs []interface{} = make([]interface{}, numberOfColumns)
			var columnsDataVals []interface{} = make([]interface{}, numberOfColumns)
			var columnValue map[string]interface{}
			columnValue = make(map[string]interface{}, numberOfColumns)

			for idx, _ := range result_set_columns[:numberOfColumns] {
				columnsDataPtrs[idx] = &columnsDataVals[idx]
			}

			if err := rows.Scan(columnsDataPtrs...); err != nil {
				log.Errorf("Failed to scan row: %v", err)
				return fiber.NewError(fiber.ErrInternalServerError.Code, "Error while parsing the data!")
			}

			for idx, colName := range result_set_columns[:numberOfColumns-1] {
				columnValue[colName] = columnsDataVals[idx]
			}
			columnValue["Time"] = columnsDataVals[numberOfColumns-1]

			columnValues = append(columnValues, columnValue)
		}
		return c.JSON(columnValues)
	}
}

func GetSingleColumn() fiber.Handler {
	return func(c fiber.Ctx) error {
		log.Infof(c.OriginalURL())
		column := c.Params("column")
		timestamp_column := c.Params("timestamp")
		timestamp_format := c.Params("format")

		if timestamp_column == "" {
			log.Error("A timestamp column must be given!")
			return fiber.NewError(fiber.ErrInternalServerError.Code, "A timestamp column must be given!")
		}
		ts_factor := utils.ToMS(timestamp_format)

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
		// Get the singleton instance of the DB driver connection.
		repository := database.GetInstance()
		db := repository.DB
		// Execute the query to read from the Parquet file and filter based on the provided parameters.
		rows, err := db.Query(fmt.Sprintf("SELECT %s, CAST(%s * %E as UBIGINT) as time FROM read_parquet('%s') WHERE time BETWEEN %d AND %d AND device_alias = '%s';", column, timestamp_column, ts_factor, repository.DataSource, fromMs, toMs, deviceAlias))
		if err != nil {
			log.Errorf("Failed to execute query: %v", err)
			return fiber.NewError(fiber.ErrInternalServerError.Code, "Failed to execute query!")
		}
		defer rows.Close()

		var columnValues []apiTypes.SimpleQueryResult
		for rows.Next() {
			var columnValue apiTypes.SimpleQueryResult

			if err := rows.Scan(&columnValue.Value, &columnValue.Time); err != nil {
				log.Errorf("Failed to scan row: %v", err)
				return fiber.NewError(fiber.ErrInternalServerError.Code, "Error while parsing the data!")
			}
			columnValues = append(columnValues, columnValue)
		}
		return c.JSON(columnValues)
	}
}
