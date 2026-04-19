package utils

import (
	"fmt"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

func ToMS(fmt string) float32 {
	var factor float32
	switch fmt {
	case "ms":
		factor = 1e0
	case "us":
		factor = 1e-3
	case "ns":
		factor = 1e-6
	case "s":
		factor = 1e3
	default:
		panic("Unknown timestamp format given, please give one from the supported list (ms, us, ns, s)!")
	}

	return factor
}

func GetCurrentTS(fmt string) int64 {
	var timestamp int64
	switch fmt {
	case "ms":
		timestamp = time.Now().UTC().UnixMilli()
	case "us":
		timestamp = time.Now().UTC().UnixMicro()
	case "ns":
		timestamp = time.Now().UTC().UnixNano()
	case "s":
		timestamp = time.Now().UTC().Unix()
	default:
		panic("Unknown timestamp format given, please give one from the supported list (ms, us, ns, s)!")
	}

	return timestamp
}

func getHiveDataSetsStrDefault(location string) (string, error) {
	files, err := filepath.Glob(filepath.Join(location, "*.parquet"))
	datasets := strings.Builder{}
	for _, dataset := range files {
		datasets.WriteString(fmt.Sprintf("'%s',", dataset))
	}
	return datasets.String(), err
}

func getHiveDataSetsStrFilter(location string, timestamp int64) (string, error) {
	files, err := filepath.Glob(filepath.Join(location, "*.parquet"))
	datasets := strings.Builder{}
	re := regexp.MustCompile(`(\d+).parquet$`)
	for _, dataset := range files {
		dataset_last_timestamp := re.FindStringSubmatch(dataset)
		if dataset_last_timestamp != nil {
			dataset_last_timestamp_int, _ := strconv.ParseInt(dataset_last_timestamp[1], 10, 64)
			if dataset_last_timestamp_int >= timestamp {
				datasets.WriteString(fmt.Sprintf("'%s',", dataset))
			}
		}
	}

	return datasets.String(), err
}

func GetHiveDataSetsStr(location string, timestamp ...int64) (string, error) {

	if len(timestamp) == 0 {
		return getHiveDataSetsStrDefault(location)
	}
	return getHiveDataSetsStrFilter(location, timestamp[0])
}
