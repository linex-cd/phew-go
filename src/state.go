package main

//api for monitor

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis"

	"github.com/shirou/gopsutil/cpu"
	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
)

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
	systemdisk := 0
	sysdiskInfo, err := disk.Usage(System_disk)
	if err == nil {
		systemdisk = int(sysdiskInfo.UsedPercent)
	}

	//datadisk
	datadisk := 0
	datadiskInfo, err := disk.Usage(Data_disk)
	if err == nil {
		datadisk = int(datadiskInfo.UsedPercent)
	}

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

	group, err1 := c.Cookie("group")
	if err1 != nil {
		group = ""
	}
	key, err2 := c.Cookie("key")
	if err2 != nil {
		key = ""
	}
	role, err3 := c.Cookie("role")
	if err3 != nil {
		role = ""
	}

	//job latest
	job_latest := make([]map[string]interface{}, 0)

	//job set of the worker role
	job_set := "job_set-" + group + "-" + key + "-" + role
	//jobs := r.ZRange(job_set, 0, -1)
	jobs, _ := r.ZRevRange(job_set, 0, -1).Result()

	for _, job_key := range jobs {

		job_key_tmp := strings.Split(job_key, "-")
		job_id := job_key_tmp[len(job_key_tmp)-1]

		item := make(map[string]interface{})

		item["job_id"] = job_id
		item["create_time"], _ = r.HGet(job_key, "create_time").Result()
		item["length"], _ = r.HGet(job_key, "length").Result()

		item["priority"], _ = r.HGet(job_key, "priority").Result()
		item["description"], _ = r.HGet(job_key, "description").Result()
		item["encrypt_job_key"] = Encrypt(job_key)

		item["create_time_i"], _ = strconv.Atoi(item["create_time"].(string))
		job_latest = append(job_latest, item)

	}

	//job_latest sort by create_time
	job_latest = Sort("create_time_i", job_latest)
	if len(job_latest) > 20 {
		job_latest = job_latest[0:19]
	}

	//task_latest
	task_latest := make([]map[string]interface{}, 0)

	tasks_pending_pattern := "tasks_pending-" + group + "-" + key + "-" + role + "-*"
	tasks_pending_set_keys, _ := r.Keys(tasks_pending_pattern).Result()

	for _, tasks_pending_set_key := range tasks_pending_set_keys {

		job_key := "job-" + tasks_pending_set_key[14:]
		job_key_tmp := strings.Split(job_key, "-")

		job_id := job_key_tmp[len(job_key_tmp)-1]

		tasks, _ := r.SMembers(tasks_pending_set_key).Result()

		job_exist, _ := r.HExists(job_key, "description").Result()

		for _, task_key := range tasks {

			if job_exist == false {
				r.SRem(tasks_pending_set_key, task_key)
				continue
			}

			item := make(map[string]interface{})

			item["job_id"] = job_id

			item["description"], _ = r.HGet(job_key, "description").Result()

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
			item["job_access_key"] = Encrypt(job_key)
			item["task_access_key"] = Encrypt(task_key)

			item["create_time_i"], _ = strconv.Atoi(item["create_time"].(string))
			task_latest = append(task_latest, item)
		}
	}

	//task_latest sort by create_time
	task_latest = Sort("create_time_i", task_latest)

	if len(task_latest) > 20 {
		task_latest = task_latest[0:19]
	}

	data := map[string][]map[string]interface{}{
		"job_latest":  job_latest,
		"task_latest": task_latest,
	}

	ResponseJson(c, 200, "ok", data)

}

func jobcounter(c *gin.Context) {

	r = getRedisInstance()

	group, err1 := c.Cookie("group")
	if err1 != nil {
		group = ""
	}
	key, err2 := c.Cookie("key")
	if err2 != nil {
		key = ""
	}
	role, err3 := c.Cookie("role")
	if err3 != nil {
		role = ""
	}

	//job_total
	statistics_job_total_key := "statistics_job_total-" + group + "-" + key + "-" + role
	statistics_job_total, _ := r.Get(statistics_job_total_key).Result()
	statistics_job_total_i, _ := strconv.Atoi(statistics_job_total)

	job_total := statistics_job_total_i

	//task_total
	statistics_task_total_key := "statistics_task_total-" + group + "-" + key + "-" + role
	statistics_task_total, _ := r.Get(statistics_task_total_key).Result()
	statistics_task_total_i, _ := strconv.Atoi(statistics_task_total)

	task_total := statistics_task_total_i

	//job_pending
	statistics_job_pending_key := "statistics_job_pending-" + group + "-" + key + "-" + role
	statistics_job_pending_pending, _ := r.Get(statistics_job_pending_key).Result()
	statistics_job_pending_pending_i, _ := strconv.Atoi(statistics_job_pending_pending)
	job_pending := statistics_job_pending_pending_i

	//work_pending
	work_key := "work-" + group + "-" + key + "-" + role
	work_pending_0, _ := r.LLen(work_key).Result()

	work_pending := int(work_pending_0)

	data := map[string]int{
		"job_total":    job_total,
		"task_total":   task_total,
		"job_pending":  job_pending,
		"work_pending": work_pending,
	}

	ResponseJson(c, 200, "ok", data)

}

func nodecounter(c *gin.Context) {

	r = getRedisInstance()

	group, err1 := c.Cookie("group")
	if err1 != nil {
		group = ""
	}
	key, err2 := c.Cookie("key")
	if err2 != nil {
		key = ""
	}
	role, err3 := c.Cookie("role")
	if err3 != nil {
		role = ""
	}

	vendor_pattern := "vendor-" + group + "-" + key + "-" + role + "-*"
	vendor_keys, _ := r.Keys(vendor_pattern).Result()
	vendor_count := len(vendor_keys)

	vendors := make([]map[string]string, 0)
	for _, vendor_key := range vendor_keys {
		item := make(map[string]string)

		items, _ := r.HGetAll(vendor_key).Result()

		for itemkey, itemvalue := range items {

			item[itemkey] = itemvalue
		}
		vendors = append(vendors, item)
	}

	worker_pattern := "worker-" + group + "-" + key + "-" + role + "-*"
	worker_keys, _ := r.Keys(worker_pattern).Result()
	worker_count := len(worker_keys)

	workers := make([]map[string]string, 0)
	for _, worker_key := range worker_keys {
		item := make(map[string]string)

		items, _ := r.HGetAll(worker_key).Result()

		for itemkey, itemvalue := range items {

			item[itemkey] = itemvalue
		}
		workers = append(workers, item)
	}

	data := map[string]interface{}{
		"vendor_count": vendor_count,
		"worker_count": worker_count,
		"vendors":      vendors,
		"workers":      workers,
	}

	ResponseJson(c, 200, "ok", data)

}

func peekjob(c *gin.Context) {

	r = getRedisInstance()

	job_access_key := c.DefaultQuery("job_access_key", "")

	if job_access_key == "" {

		ResponseJson(c, 404, "forbidden", make(map[string]string))
		return
	}

	job_key := Decrypt(job_access_key)

	job_exist, _ := r.HExists(job_key, "state").Result()

	if job_exist == false {
		ResponseJson(c, 404, "job not found", make(map[string]string))
		return
	}

	data := make(map[string]string)

	job_key_tmp := strings.Split(job_key, "-")
	job_id := job_key_tmp[len(job_key_tmp)-1]

	data["job_id"] = job_id

	data["state"], _ = r.HGet(job_key, "state").Result()

	data["create_time"], _ = r.HGet(job_key, "create_time").Result()
	data["finish_time"], _ = r.HGet(job_key, "finish_time").Result()

	data["vendor_id"], _ = r.HGet(job_key, "vendor_id").Result()
	data["worker_group"], _ = r.HGet(job_key, "worker_group").Result()

	data["meta"], _ = r.HGet(job_key, "meta").Result()
	data["description"], _ = r.HGet(job_key, "description").Result()
	data["priority"], _ = r.HGet(job_key, "priority").Result()

	data["length"], _ = r.HGet(job_key, "length").Result()

	ResponseJson(c, 200, "ok", data)
}

func peektask(c *gin.Context) {

	r = getRedisInstance()

	task_access_key := c.DefaultQuery("task_access_key", "")

	if task_access_key == "" {

		ResponseJson(c, 404, "forbidden", make(map[string]string))
		return
	}

	task_key := Decrypt(task_access_key)

	task_exist, _ := r.HExists(task_key, "state").Result()

	if task_exist == false {
		ResponseJson(c, 404, "task not found", make(map[string]string))
		return
	}

	data := make(map[string]string)

	data["state"], _ = r.HGet(task_key, "state").Result()
	data["note"], _ = r.HGet(task_key, "note").Result()
	//data["result"], _ = r.HGet(task_key, "result").Result()

	data["create_time"], _ = r.HGet(task_key, "create_time").Result()
	data["start_time"], _ = r.HGet(task_key, "start_time").Result()
	data["finish_time"], _ = r.HGet(task_key, "finish_time").Result()

	data["job_id"], _ = r.HGet(task_key, "job_id").Result()
	data["priority"], _ = r.HGet(task_key, "priority").Result()

	data["meta"], _ = r.HGet(task_key, "meta").Result()
	data["addressing"], _ = r.HGet(task_key, "addressing").Result()
	data["port"], _ = r.HGet(task_key, "port").Result()

	ResponseJson(c, 200, "ok", data)
}

func peekfile(c *gin.Context) {

	filename := c.DefaultQuery("filename", "")
	if filename == "" {
		c.Redirect(http.StatusMovedPermanently, "/monitor/")
	}

	//只访问特定目录
	if strings.Contains(filename, URI_dir) == false {
		c.Redirect(http.StatusMovedPermanently, "/monitor/")
	}

	//禁止伪造目录
	if strings.Contains(filename, "..") == true {
		c.Redirect(http.StatusMovedPermanently, "/monitor/")
	}

	if Existfile(filename) == false {
		c.Redirect(http.StatusMovedPermanently, "/monitor/")
	}

	ResponseFile(c, filename)
}

func percentage(c *gin.Context) {

	r = getRedisInstance()

	group, err1 := c.Cookie("group")
	if err1 != nil {
		group = ""
	}
	key, err2 := c.Cookie("key")
	if err2 != nil {
		key = ""
	}
	role, err3 := c.Cookie("role")
	if err3 != nil {
		role = ""
	}

	//addressing_count
	addressing_data := make(map[string]int)

	statistics_task_addressing_pattern := "statistics_task_addressing-" + group + "-" + key + "-" + role + "-*"
	statistics_task_addressing_keys, _ := r.Keys(statistics_task_addressing_pattern).Result()

	for _, statistics_task_addressing_key := range statistics_task_addressing_keys {

		addressing_tmp := strings.Split(statistics_task_addressing_key, "-")
		addressing := addressing_tmp[len(addressing_tmp)-1]

		addressing_count_tmp, _ := r.Get(statistics_task_addressing_key).Result()
		addressing_count, _ := strconv.Atoi(addressing_count_tmp)

		if _, ok := addressing_data[addressing]; !ok {
			addressing_data[addressing] = 0
		}
		addressing_data[addressing] = addressing_data[addressing] + addressing_count

	}

	//port_count
	port_data := make(map[string]int)

	statistics_task_port_pattern := "statistics_task_port-" + group + "-" + key + "-" + role + "-*"
	statistics_task_port_keys, _ := r.Keys(statistics_task_port_pattern).Result()

	for _, statistics_task_port_key := range statistics_task_port_keys {

		port_tmp := strings.Split(statistics_task_port_key, "-")
		port := port_tmp[len(port_tmp)-1]

		port_count_tmp, _ := r.Get(statistics_task_port_key).Result()
		port_count, _ := strconv.Atoi(port_count_tmp)

		if _, ok := port_data[port]; !ok {
			port_data[port] = 0
		}
		port_data[port] = port_data[port] + port_count

	}

	data := map[string]interface{}{
		"addressing": addressing_data,
		"port":       port_data,
	}

	ResponseJson(c, 200, "ok", data)
}

func errorlist(c *gin.Context) {

	r = getRedisInstance()

	group, err1 := c.Cookie("group")
	if err1 != nil {
		group = ""
	}
	key, err2 := c.Cookie("key")
	if err2 != nil {
		key = ""
	}
	role, err3 := c.Cookie("role")
	if err3 != nil {
		role = ""
	}

	//error job list
	error_jobs := make([]map[string]interface{}, 0)

	error_job_set_key := "error_job-" + group + "-" + key + "-" + role

	time_now := int(time.Now().Unix())
	error_ttl, _ := strconv.Atoi(Error_TTL)
	time_ttl := time_now - error_ttl

	zrangeby := redis.ZRangeBy{
		Min: strconv.Itoa(time_ttl),
		Max: strconv.Itoa(time_now),
	}

	job_keys, _ := r.ZRangeByScore(error_job_set_key, zrangeby).Result()

	//remove expired keys
	r.ZRemRangeByScore(error_job_set_key, strconv.Itoa(0), strconv.Itoa(time_ttl-1))

	for _, job_key := range job_keys {

		item := make(map[string]interface{})

		job_key_tmp := strings.Split(job_key, "-")

		job_id := job_key_tmp[len(job_key_tmp)-1]

		item["job_id"] = job_id
		item["create_time"], _ = r.HGet(job_key, "create_time").Result()
		item["length"], _ = r.HGet(job_key, "length").Result()

		item["priority"], _ = r.HGet(job_key, "priority").Result()
		item["description"], _ = r.HGet(job_key, "description").Result()
		item["encrypt_job_key"] = Encrypt(job_key)

		item["create_time_i"], _ = strconv.Atoi(item["create_time"].(string))
		error_jobs = append(error_jobs, item)

	}

	//error jobs sort by create_time
	error_jobs = Sort("create_time_i", error_jobs)
	if len(error_jobs) > 20 {
		error_jobs = error_jobs[0:19]
	}

	//error task list

	error_tasks := make([]map[string]interface{}, 0)

	error_task_set_key := "error_task-" + group + "-" + key + "-" + role

	time_now_2 := int(time.Now().Unix())
	error_ttl_2, _ := strconv.Atoi(Error_TTL)
	time_ttl_2 := time_now_2 - error_ttl_2

	zrangeby_2 := redis.ZRangeBy{
		Min: strconv.Itoa(time_ttl_2),
		Max: strconv.Itoa(time_now_2),
	}

	tasks, _ := r.ZRangeByScore(error_task_set_key, zrangeby_2).Result()

	//remove expired keys
	r.ZRemRangeByScore(error_task_set_key, strconv.Itoa(0), strconv.Itoa(time_ttl_2-1))

	for _, task_key := range tasks {
		worker_tmp := strings.Split(task_key, "-")
		worker_group := worker_tmp[1]
		worker_key := worker_tmp[2]
		worker_role := worker_tmp[3]
		job_id := worker_tmp[4]

		job_key := "job-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_id

		job_exist, _ := r.HExists(job_key, "description").Result()

		if job_exist == false {
			r.ZRem(error_task_set_key, task_key)
			continue
		}

		item := make(map[string]interface{})

		item["job_id"] = job_id

		item["description"], _ = r.HGet(job_key, "description").Result()

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
		item["job_access_key"] = Encrypt(job_key)
		item["task_access_key"] = Encrypt(task_key)

		item["create_time_i"], _ = strconv.Atoi(item["create_time"].(string))
		error_tasks = append(error_tasks, item)
	}

	//error tasks sort by create_time
	error_tasks = Sort("create_time_i", error_tasks)

	if len(error_tasks) > 20 {
		error_tasks = error_tasks[0:19]
	}

	data := map[string][]map[string]interface{}{
		"error_jobs":  error_jobs,
		"error_tasks": error_tasks,
	}

	ResponseJson(c, 200, "ok", data)

}
