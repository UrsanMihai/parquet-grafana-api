package handlers

import (
	"fmt"
	"strconv"
	"strings"

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

		fromTS, err := strconv.ParseInt(c.Query("from"), 10, 64)
		if err != nil {
			log.Errorf("Bad query param format, from timestamp! 0 will be used as a default value for from query param.")
			fromTS = 0
		}
		toTS, err := strconv.ParseInt(c.Query("to"), 10, 64)
		if err != nil {
			log.Errorf("Bad query param format, to timestamp! Current timestamp will be used as a default value for to query param.")
			toTS = utils.GetCurrentTS(timestamp_format)
		}
		deviceAlias := c.Query("device_alias")
		// Get the singleton instance of the DB driver connection.
		repository := database.GetInstance()
		db := repository.DB

		// Create the query string.
		queryBuilder := strings.Builder{}
		if repository.HiveParquetSource != "" {
			hiveDataSets := ""
			switch repository.UseTemporalFilter {
			case true:
				hiveDataSets, _ = utils.GetHiveDataSetsStr(repository.HiveParquetSource, fromTS)
				break
			case false:
				hiveDataSets, _ = utils.GetHiveDataSetsStr(repository.HiveParquetSource)
				break
			}

			if hiveDataSets != "" {
				queryBuilder.WriteString(fmt.Sprintf("SELECT %s, CAST(%s * %E as UBIGINT) as time FROM read_parquet([%s]) WHERE %s BETWEEN %d AND %d AND device_alias = '%s'", column, timestamp_column, ts_factor, hiveDataSets, timestamp_column, fromTS, toTS, deviceAlias))
			}
		}

		if repository.SimpleParquetSource != "" {
			if queryBuilder.Len() != 0 {
				queryBuilder.WriteString(" UNION ")
			}
			queryBuilder.WriteString(fmt.Sprintf("SELECT %s, CAST(%s * %E as UBIGINT) as time FROM read_parquet(['%s']) WHERE %s BETWEEN %d AND %d AND device_alias = '%s';", column, timestamp_column, ts_factor, repository.SimpleParquetSource+"/*.parquet", timestamp_column, fromTS, toTS, deviceAlias))
		} else {
			queryBuilder.WriteString(";")
		}
		// Execute the query to read from the data sources and filter based on the provided parameters.
		query := queryBuilder.String()
		log.Infof("Executing query: %s", query)
		rows, err := db.Query(query)
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

		fromTS, err := strconv.ParseInt(c.Query("from"), 10, 64)
		if err != nil {
			log.Errorf("Bad query param format, from timestamp! 0 will be used as a default value for from query param.")
			fromTS = 0
		}
		toTS, err := strconv.ParseInt(c.Query("to"), 10, 64)
		if err != nil {
			log.Errorf("Bad query param format, to timestamp! Current timestamp will be used as a default value for to query param.")
			toTS = utils.GetCurrentTS(timestamp_format)
		}
		deviceAlias := c.Query("device_alias")
		// Get the singleton instance of the DB driver connection.
		repository := database.GetInstance()
		db := repository.DB

		// Create the query string.
		queryBuilder := strings.Builder{}
		if repository.HiveParquetSource != "" {
			hiveDataSets := ""
			switch repository.UseTemporalFilter {
			case true:
				hiveDataSets, _ = utils.GetHiveDataSetsStr(repository.HiveParquetSource, fromTS)
				break
			case false:
				hiveDataSets, _ = utils.GetHiveDataSetsStr(repository.HiveParquetSource)
				break
			}

			if hiveDataSets != "" {
				queryBuilder.WriteString(fmt.Sprintf("SELECT %s, CAST(%s * %E as UBIGINT) as time FROM read_parquet([%s]) WHERE %s BETWEEN %d AND %d AND device_alias = '%s'", column, timestamp_column, ts_factor, hiveDataSets, timestamp_column, fromTS, toTS, deviceAlias))
			}
		}
		if repository.SimpleParquetSource != "" {
			if queryBuilder.Len() != 0 {
				queryBuilder.WriteString(" UNION ")
			}
			queryBuilder.WriteString(fmt.Sprintf("SELECT %s, CAST(%s * %E as UBIGINT) as time FROM read_parquet(['%s']) WHERE %s BETWEEN %d AND %d AND device_alias = '%s'", column, timestamp_column, ts_factor, repository.SimpleParquetSource+"/*.parquet", timestamp_column, fromTS, toTS, deviceAlias))
		} else {
			queryBuilder.WriteString(";")
		}
		// Execute the query to read from the Parquet file and filter based on the provided parameters.
		rows, err := db.Query(queryBuilder.String())
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
