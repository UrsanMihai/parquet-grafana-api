# parquet-grafana-api (infinity datasource)

## Details

API used to convert data from custom **parquet** files into **json** format used by grafana infinity datasource.
This API is using DuckDB Go client to query the source and to retrive the data in the required format.

### Requirements:

- [Golang 1.26.0](https://go.dev/doc/go1.26).
- [Go fiber](https://gofiber.io/) (for API implementation).
- [Cobra](https://cobra.dev/) (for CLI implementation).
- [DuckDB Go client](https://duckdb.org/docs/stable/clients/go) to interact with data source via a SQL interface.

### Usage

#### How to start the server:

```bash
parquet-grafana-api.exe server [flags]
```

Flags:

Flags:

- -H, --Host string Host to run the server on. (default "localhost")
- -P, --Port string Port to run the server on. (default "3000")
- -h, --help help for server
- -f, --hive_parquet_location string Path to the directory which contains at least one parquet file with the Hive data schema. (Optional)
- -s, --simple_parquet_location string Path to the directory which contains at least one parquet file with the simple data schema. (Optional)
- -t, --use_temporal_filter Whether to optimize queries on hive sets using temporal filtering.

#### How to consume the API:

- Via [infinity](https://grafana.com/grafana/plugins/yesoreyeram-infinity-datasource/) data plugin for [Grafana](https://grafana.com/), to make the data available into a Grafana Dashboard.
- Define the API URLs into your grafana panels from your dashboard.
- If someone wants to visualize multiple columns, without taking care about a user defined mapping, **UQL** Frontend parser can be used.
- If someone wants to define a known mapping for the visualization component **JSONata** parser can be used from infinity configuration.
