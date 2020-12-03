package main

//api for monitor

import (
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis"
	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
)

var r *redis.Client = nil

func getRedisInstance() *redis.Client {
	if r == nil {
		r = redis.NewClient(&redis.Options{
			Addr:     Redis_host + ":2019",
			Password: "",
			DB:       0,
		})

	}

	return r
}

func system(c *gin.Context) {

	//REDIS DB  1G
	db := int(Filesize(Data_dir+"redis.rdb") / (1024 * 1024 * 1024 * 1) * 100)

	//CPU
	percent, _ := cpu.Percent(time.Second, false)
	cpu := int(percent[0])

	//memory
	memInfo, _ := mem.VirtualMemory()
	memory := int(memInfo.UsedPercent)

	//systemdisk
	sysdiskInfo, _ := disk.Usage(System_disk)
	systemdisk := int(sysdiskInfo.UsedPercent)

	//datadisk
	datadiskInfo, _ := disk.Usage(Data_disk)
	datadisk := int(datadiskInfo.UsedPercent)

	//GPU
	gpu := 90

	//temp
	temp := 60

	data := map[string]int{
		"db":         db,
		"cpu":        cpu,
		"memory":     memory,
		"gpu":        gpu,
		"systemdisk": systemdisk,
		"datadisk":   datadisk,
		"temp":       temp,
	}

	ResponseJson(c, 200, "ok", data)

}

func latestwork(c *gin.Context) {

	r = getRedisInstance()
	_, err := r.Ping().Result()
	if err != nil {
		panic(err)
	}

	//job latest
	job_latest := make([]map[string]interface{}, 0)

	job_total_pattern := "job-*"
	jobs, _ := r.Keys(job_total_pattern).Result()

	for _, jobKey := range jobs {

		jobKey_tmp := strings.Split(jobKey, "-")
		job_id := jobKey_tmp[len(jobKey_tmp)-1]

		item := make(map[string]interface{})

		item["job_id"] = job_id
		item["create_time"], _ = r.HGet(jobKey, "create_time").Result()
		item["length"], _ = r.HGet(jobKey, "length").Result()

		item["priority"], _ = r.HGet(jobKey, "priority").Result()
		item["description"], _ = r.HGet(jobKey, "description").Result()
		item["encrypt_jobKey"] = Encrypt(jobKey)

		job_latest = append(job_latest, item)

	}

	//job_latest sort by create_time
	job_latest = Sort("create_time", job_latest)
	if len(job_latest) > 20 {
		job_latest = job_latest[0:19]
	}

	//task_latest
	task_latest := make([]map[string]interface{}, 0)

	tasks_pending_pattern := "tasks_pending-*"
	tasks_pending_set_keys, _ := r.Keys(tasks_pending_pattern).Result()

	for _, tasks_pending_set_key := range tasks_pending_set_keys {

		jobKey := "job-" + tasks_pending_set_key[14:]
		jobKey_tmp := strings.Split(jobKey, "-")

		job_id := jobKey_tmp[len(jobKey_tmp)-1]

		tasks, _ := r.SMembers(tasks_pending_set_key).Result()

		job_exist, _ := r.HExists(jobKey, "description").Result()

		for _, task_key := range tasks {

			if job_exist == false {
				r.SRem(tasks_pending_set_key, task_key)
				continue
			}

			item := make(map[string]interface{})

			item["job_id"] = job_id

			item["description"], _ = r.HGet(jobKey, "description").Result()

			start_time_exist, _ := r.HExists(task_key, "start_time").Result()
			if start_time_exist == false {
				//ignore deleted task
				continue
			}

			addressing, _ := r.HGet(task_key, "addressing").Result()
			if addressing == "binary" {
				item["data"] = "BINARY"
			} else {
				item["data"], _ = r.HGet(task_key, "data").Result()
			}

			item["port"], _ = r.HGet(task_key, "port").Result()
			item["addressing"] = addressing
			item["create_time"], _ = r.HGet(task_key, "create_time").Result()
			item["start_time"], _ = r.HGet(task_key, "start_time").Result()
			item["job_access_key"] = Encrypt(jobKey)
			item["task_access_key"] = Encrypt(task_key)

			task_latest = append(task_latest, item)
		}
	}

	//task_latest sort by create_time
	task_latest = Sort("create_time", task_latest)

	if len(task_latest) > 20 {
		task_latest = job_latest[0:19]
	}

	data := map[string][]map[string]interface{}{
		"job_latest":  job_latest,
		"task_latest": task_latest,
	}

	ResponseJson(c, 200, "ok", data)

}

func jobcounter(c *gin.Context) {

	r = getRedisInstance()
	_, err := r.Ping().Result()
	if err != nil {
		panic(err)
	}

	//job_total
	statistics_job_total_pattern := "statistics_job_total-*"
	statistics_job_total_pending_keys, _ := r.Keys(statistics_job_total_pattern).Result()

	job_total := 0
	for _, statistics_job_total_pending_key := range statistics_job_total_pending_keys {
		statistics_job_total_pending, _ := r.Get(statistics_job_total_pending_key).Result()
		statistics_job_total_pending_i, _ := strconv.Atoi(statistics_job_total_pending)
		job_total = job_total + statistics_job_total_pending_i
	}

	//task_total
	statistics_task_total_pattern := "statistics_task_total-*"
	statistics_task_total_pending_keys, _ := r.Keys(statistics_task_total_pattern).Result()

	task_total := 0
	for _, statistics_task_total_pending_key := range statistics_task_total_pending_keys {
		statistics_task_total_pending, _ := r.Get(statistics_task_total_pending_key).Result()
		statistics_task_total_pending_i, _ := strconv.Atoi(statistics_task_total_pending)
		task_total = task_total + statistics_task_total_pending_i

	}

	//job_pending
	statistics_job_pending_pattern := "statistics_job_pending-*"
	statistics_job_pending_pending_keys, _ := r.Keys(statistics_job_pending_pattern).Result()

	job_pending := 0
	for _, statistics_job_pending_pending_key := range statistics_job_pending_pending_keys {
		statistics_job_pending_pending, _ := r.Get(statistics_job_pending_pending_key).Result()
		statistics_job_pending_pending_i, _ := strconv.Atoi(statistics_job_pending_pending)
		job_pending = job_pending + statistics_job_pending_pending_i
	}

	//work_pending
	work_pattern := "work-*"
	work_pending_keys, _ := r.Keys(work_pattern).Result()

	work_pending := 0
	for _, work_pending_key := range work_pending_keys {
		work_pending_0, _ := r.LLen(work_pending_key).Result()
		work_pending = work_pending + int(work_pending_0)
	}

	data := map[string]int{
		"job_total":    job_total,
		"task_total":   task_total,
		"job_pending":  job_pending,
		"work_pending": work_pending,
	}

	ResponseJson(c, 200, "ok", data)

}
