package main

import (
	"fmt"
	"strconv"
	"time"
)

//daemon for timeout task

//timeout = 300

func health_thread(timeout int64) {

	r = getRedisInstance()

	fmt.Println("started health thread, timeout = ", timeout)

	for {

		time.Sleep(30)
		vendor_pattern := "vendor-*"
		vendor_keys, _ := r.Keys(vendor_pattern).Result()

		for _, vendor_key := range vendor_keys {
			ping_time, _ := r.HGet(vendor_key, "ping_time").Result()
			ping_time_i32, _ := strconv.Atoi(ping_time)
			ping_time_i := int64(ping_time_i32)

			state, _ := r.HGet(vendor_key, "state").Result()
			if state == "online" {
				if time.Now().Unix()-ping_time_i > timeout {
					r.HSet(vendor_key, "state", "offline")
				}
			}
			if state == "offline" {
				if time.Now().Unix()-ping_time_i < timeout {
					r.HSet(vendor_key, "state", "online")
				}
			}
		}

		worker_pattern := "worker-*"
		worker_keys, _ := r.Keys(worker_pattern).Result()

		for _, worker_key := range worker_keys {
			ping_time, _ := r.HGet(worker_key, "ping_time").Result()
			ping_time_i32, _ := strconv.Atoi(ping_time)
			ping_time_i := int64(ping_time_i32)

			state, _ := r.HGet(worker_key, "state").Result()
			if state == "online" {
				if time.Now().Unix()-ping_time_i > timeout {
					r.HSet(worker_key, "state", "offline")
				}
			}
			if state == "offline" {
				if time.Now().Unix()-ping_time_i < timeout {
					r.HSet(worker_key, "state", "online")
				}
			}
		}

	}

}
