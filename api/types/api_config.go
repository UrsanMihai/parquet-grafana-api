package types

import "github.com/gofiber/fiber/v3/log"

type APIConfig struct {
	DataSource string
	Port       string
}

func (cfg *APIConfig) AreValid() bool {
	if cfg.DataSource == "" {
		log.Error("Please provide the path to the Parquet file using --parquet_path")
		return false
	}
	if cfg.Port == "" {
		log.Error("Please provide the port of the server using --port port_number")
		return false
	}
	return true
}
