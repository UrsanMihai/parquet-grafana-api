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
	hive_parquet_location, _ := cmd.Flags().GetString("hive_parquet_location")
	simple_parquet_location, _ := cmd.Flags().GetString("simple_parquet_location")
	use_temporal_filter, _ := cmd.Flags().GetBool("use_temporal_filter")

	cfg := types.APIConfig{
		Port:                  port,
		Host:                  host,
		HiveParquetLocation:   hive_parquet_location,
		SimpleParquetLocation: simple_parquet_location,
		UseTemporalFilter:     use_temporal_filter,
	}
	cfg.AreValid()
	api.Main(&cfg)
}

var cmd = &cobra.Command{
	Use:   "server",
	Short: "Main server command to launch the grafana parquet plugin.",
	Run:   exec,
}

func Execute() {
	cmd.Flags().StringP("Host", "H", "localhost", "Host to run the server on.")
	cmd.Flags().StringP("Port", "P", "3000", "Port to run the server on.")
	cmd.Flags().StringP("hive_parquet_location", "f", "", "Path to the directory which contains at least one parquet file with the Hive data schema. (Optional)")
	cmd.Flags().StringP("simple_parquet_location", "s", "", "Path to the directory which contains at least one parquet file with the simple data schema. (Optional)")
	cmd.Flags().BoolP("use_temporal_filter", "t", false, "Whether to optimize queries on hive sets using temporal filtering.")
	if err := cmd.Execute(); err != nil {
		log.Errorf("Failed to execute command: %v", err)
	}
}
