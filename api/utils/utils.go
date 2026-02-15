package utils

import "time"

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
