package types

import (
	"fmt"
	"os"

	"github.com/gofiber/fiber/v3/log"
)

type APIConfig struct {
	HiveParquetLocation   string
	SimpleParquetLocation string
	Host                  string
	Port                  string
	UseTemporalFilter     bool
}

func (cfg *APIConfig) AreValid() bool {
	_, err := os.Stat(cfg.HiveParquetLocation)
	if cfg.HiveParquetLocation != "" && os.IsNotExist(err) {
		log.Error(fmt.Sprintf("Please provide valid hive parquet parent directory. Invalid path: %s", cfg.HiveParquetLocation))
		return false
	}

	_, err = os.Stat(cfg.SimpleParquetLocation)
	if cfg.SimpleParquetLocation != "" && os.IsNotExist(err) {
		log.Error(fmt.Sprintf("Please provide valid simple parquet parent directory. Invalid path: %s", cfg.SimpleParquetLocation))
		return false
	}

	if cfg.Host == "" {
		cfg.Host = "localhost"
		log.Warn("No host provided, using default host: localhost")
	}

	if cfg.Port == "" {
		cfg.Port = "3000"
		log.Warn("No port provided, using default port: 3000")
	}
	return true
}
