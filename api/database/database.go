package database

import (
	"database/sql"
	"sync"

	"github.com/gofiber/fiber/v3/log"
	apiTypes "github.com/ursanmihai/parquet-grafana-api/api/types"
)

type repository struct {
	DB                  *sql.DB
	HiveParquetSource   string
	SimpleParquetSource string
	UseTemporalFilter   bool
}

var instance *repository
var poolOnce sync.Once

func Init(driver string, apiConfig *apiTypes.APIConfig) (*repository, error) {
	var db *sql.DB
	var err error
	poolOnce.Do(
		func() {
			db, err = sql.Open(driver, "")
		})

	if err != nil {
		log.Errorf("Error while connecting to the DB")
		return nil, err
	}

	instance = &repository{
		DB:                  db,
		HiveParquetSource:   apiConfig.HiveParquetLocation,
		SimpleParquetSource: apiConfig.SimpleParquetLocation,
		UseTemporalFilter:   apiConfig.UseTemporalFilter,
	}

	return instance, nil
}

func GetInstance() *repository {
	if instance == nil {
		panic("Database should be initialized before getting an instance of it!")
	}
	return instance
}

func (r *repository) Close() {
	r.DB.Close()
}
