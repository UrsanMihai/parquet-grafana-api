package cmd

import (
	_ "github.com/duckdb/duckdb-go/v2"
	"github.com/gofiber/fiber/v3/log"
	"github.com/spf13/cobra"
	"github.com/ursanmihai/parquet-grafana-api/api"
	"github.com/ursanmihai/parquet-grafana-api/api/types"
)

func exec(cmd *cobra.Command, args []string) {
	// Check the CLI flags.
	parquet_path, _ := cmd.Flags().GetString("parquet_path")
	port, _ := cmd.Flags().GetString("port")

	cfg := types.APIConfig{
		Port:       port,
		DataSource: parquet_path,
	}
	cfg.AreValid()
	api.Main(cfg)
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
