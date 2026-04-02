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
	host, _ := cmd.Flags().GetString("Host")
	port, _ := cmd.Flags().GetString("Port")
	parquet_sources, _ := cmd.Flags().GetStringSlice("parquet_paths")
	temp_sources, _ := cmd.Flags().GetStringSlice("temp_paths")

	cfg := types.APIConfig{
		Port:            port,
		Host:            host,
		DataSources:     parquet_sources,
		TempDataSources: temp_sources,
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
	cmd.Flags().StringP("Host", "H", "localhost", "Host to run the server on.")
	cmd.Flags().StringP("Port", "P", "3000", "Port to run the server on.")
	cmd.Flags().StringSliceP("parquet_paths", "f", []string{}, "Paths to the parquet data sources.")
	cmd.Flags().StringSliceP("temp_paths", "t", []string{}, "Paths to the temporary data sources. (Optional)")
	if err := cmd.Execute(); err != nil {
		log.Errorf("Failed to execute command: %v", err)
	}
}
