package types

import (
	"fmt"
	"os"

	"github.com/gofiber/fiber/v3/log"
)

type APIConfig struct {
	DataSources     []string
	TempDataSources []string
	Host            string
	Port            string
}

func (cfg *APIConfig) AreValid() bool {
	if len(cfg.DataSources) == 0 {
		log.Error("Please provide at least one parquet data source --parquet_paths path1,path2,...")
		return false
	} else {
		for _, path := range cfg.DataSources {
			_, err := os.Stat(path)
			if path == "" && os.IsNotExist(err) {
				log.Error(fmt.Sprintf("Please provide valid parquet data source paths. Invalid path: %s", path))
				return false
			}
		}
	}

	for _, tempPath := range cfg.TempDataSources {
		_, err := os.Stat(tempPath)
		if tempPath == "" && os.IsNotExist(err) {
			log.Error(fmt.Sprintf("Please provide valid temporary data source paths. Invalid path: %s", tempPath))
			return false
		}
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
