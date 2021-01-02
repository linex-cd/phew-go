package main

import (
	"os"
	"runtime"
)

//phew config
var Redis_host = os.Getenv("PHEW_REDIS_HOST")

var Data_dir = os.Getenv("PHEW_DATA_DIR")

var System_disk = os.Getenv("PHEW_SYSTEM_DISK")

var Data_disk = os.Getenv("PHEW_DATA_DISK")

var URI_dir = os.Getenv("PHEW_URI_DIR")

var Error_TTL = os.Getenv("PHEW_ERROR_TTL")

var OsType = runtime.GOOS

func init() {

	if OsType == "windows" {
		if Redis_host == "" {
			Redis_host = "127.0.0.1"
		}

		if Data_dir == "" {
			Data_dir = "../phewdata/"
		}

		if System_disk == "" {
			System_disk = "C:/"
		}

		if Data_disk == "" {
			Data_disk = "D:/"
		}

		if URI_dir == "" {
			URI_dir = "/uri/"
		}

		if Error_TTL == "" {
			Error_TTL = "259200" //3600*24*3 , 3天
		}

	} else {

		if Redis_host == "" {
			Redis_host = "127.0.0.1"
		}

		if Data_disk == "" {
			Data_disk = "/phewdata/"
		}

		if System_disk == "" {
			System_disk = "/"
		}

		if URI_dir == "" {
			URI_dir = "../data/"
		}

		if Error_TTL == "" {
			Error_TTL = "259200" //3600*24*3 , 3天
		}

	}

}
