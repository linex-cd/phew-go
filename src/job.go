package main

//api for worker

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func jobping(c *gin.Context) {

	r = getRedisInstance()
	_, err := r.Ping().Result()
	if err != nil {
		panic(err)
	}

	rawdata, err := c.GetRawData()
	if err != nil {
		ResponseJson(c, 400, "error", "bad request")
		return
	}

	var jsondata map[string]interface{}

	json.Unmarshal([]byte(rawdata), &jsondata)

	vendor_id := jsondata["vendor_id"].(string)

	worker_group := jsondata["worker_group"].(string)
	worker_key := jsondata["worker_key"].(string)
	worker_role := jsondata["worker_role"].(string)

	vendor_node_key := "vendor-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + vendor_id

	vendor_node_ip := c.ClientIP()

	//set to online
	r.HSet(vendor_node_key, "ping_time", time.Now().Unix())
	r.HSet(vendor_node_key, "ip", vendor_node_ip)

	r.HSet(vendor_node_key, "vendor_id", jsondata["vendor_id"].(string))
	r.HSet(vendor_node_key, "name", jsondata["vendor_name"].(string))
	r.HSet(vendor_node_key, "location", jsondata["vendor_location"].(string))
	r.HSet(vendor_node_key, "state", "online")

	data := "pong"

	ResponseJson(c, 200, "ok", data)

}

func assign(c *gin.Context) {

	r = getRedisInstance()
	_, err := r.Ping().Result()
	if err != nil {
		panic(err)
	}

	rawdata, err := c.GetRawData()
	if err != nil {
		ResponseJson(c, 400, "error", "bad request")
		return
	}

	var jsondata map[string]interface{}

	json.Unmarshal([]byte(rawdata), &jsondata)

	worker_group := jsondata["worker_group"].(string)
	worker_key := jsondata["worker_key"].(string)
	worker_role := jsondata["worker_role"].(string)

	job_info := jsondata["job"].(map[string]interface{})

	//make job record
	job_key := "job-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"].(string)
	r.HSet(job_key, "state", "assigned")

	//add create timestamp
	r.HSet(job_key, "create_time", time.Now().Unix())
	r.HSet(job_key, "finish_time", "")

	r.HSet(job_key, "vendor_id", jsondata["vendor_id"])

	r.HSet(job_key, "worker_group", jsondata["worker_group"])
	r.HSet(job_key, "worker_key", jsondata["worker_key"])
	r.HSet(job_key, "worker_role", jsondata["worker_role"])

	r.HSet(job_key, "meta", job_info["meta"].(string))
	r.HSet(job_key, "description", job_info["description"].(string))
	r.HSet(job_key, "priority", job_info["priority"].(string))

	tasks := jsondata["tasks"].([]map[string]string)

	length := int64(len(tasks))
	r.HSet(job_key, "length", length)

	//job and task count statistics
	//job total
	statistics_job_total_key := "statistics_job_total-" + worker_group + "-" + worker_key + "-" + worker_role
	r.IncrBy(statistics_job_total_key, 1)

	//job pending
	statistics_job_pending_key := "statistics_job_pending-" + worker_group + "-" + worker_key + "-" + worker_role
	r.IncrBy(statistics_job_pending_key, 1)

	//task total
	statistics_task_total_key := "statistics_task_total-" + worker_group + "-" + worker_key + "-" + worker_role
	r.IncrBy(statistics_task_total_key, length)

	//type base
	statistics_task_addressing_key_base := "statistics_task_addressing-" + worker_group + "-" + worker_key + "-" + worker_role

	statistics_task_port_key_base := "statistics_task_port-" + worker_group + "-" + worker_key + "-" + worker_role

	//save task records
	ignore_count := int64(0)

	for _, task_info := range tasks {

		//make task records

		task_info["hash"] = Md5(task_info["data"])

		task_key := "task-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"].(string) + "-" + task_info["hash"]

		r.HSet(task_key, "state", "assigned")
		r.HSet(task_key, "note", "")
		r.HSet(task_key, "result", "")

		// add create timestamp
		r.HSet(task_key, "create_time", time.Now().Unix())
		r.HSet(task_key, "start_time", "")
		r.HSet(task_key, "finish_time", "")
		r.HSet(task_key, "try_times", 0)

		r.HSet(task_key, "job_id", job_info["job_id"])
		r.HSet(task_key, "priority", job_info["priority"])

		r.HSet(task_key, "hash", task_info["hash"])

		r.HSet(task_key, "meta", task_info["meta"])
		r.HSet(task_key, "addressing", task_info["addressing"])
		r.HSet(task_key, "port", task_info["port"])

		//task addressing and port count statistics
		statistics_task_addressing_key := statistics_task_addressing_key_base + "-" + task_info["addressing"]
		r.IncrBy(statistics_task_addressing_key, 1)

		statistics_task_port_key := statistics_task_port_key_base + "-" + task_info["port"]
		r.IncrBy(statistics_task_port_key, 1)

		//skip ignore task
		if task_info["port"] == "ignore" {
			ignore_count = ignore_count + 1
			r.HSet(task_key, "state", "done")
			r.HSet(task_key, "note", "ignore file")
			r.HSet(task_key, "finish_time", time.Now().Unix())
			r.HSet(task_key, "result", "")
			if ignore_count == length {
				//add a job to set as unread
				jobs_done_key := "jobs_done-" + worker_group + "-" + worker_key + "-" + worker_role

				r.SAdd(jobs_done_key, task_info["job_id"])

				//add finish timestamp
				r.HSet(job_key, "finish_time", time.Now().Unix())
				r.HSet(job_key, "state", "done")

				//job pending statistics
				r.DecrBy(statistics_job_pending_key, 1)

				statistics_job_pending_count_s, _ := r.Get(statistics_job_pending_key).Result()
				statistics_job_pending_count_i, _ := strconv.Atoi(statistics_job_pending_count_s)
				if statistics_job_pending_count_i < 0 {
					r.Set(statistics_job_pending_key, "0", -1)
				}

			}
			continue
		}

		//save binary to disk for tmp use
		if task_info["addressing"] == "binary" {
			taskdata_filename := Filedirfromhash(task_info["hash"]) + task_info["hash"] + ".taskdata"
			Makedirforhash(task_info["hash"])
			Writefile(taskdata_filename, task_info["data"], "w")
			task_info["data"] = ""
		} else {
			r.HSet(task_key, "data", task_info["data"])
		}

		//allocate task to work priority list
		work_key := "work-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["priority"].(string)
		r.LPush(work_key, task_key)

		//add task to tasks_waiting to wait for job state check
		tasks_waiting_key := "tasks_waiting-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"].(string)
		r.SAdd(tasks_waiting_key, task_key)

	}

	//update vendor node hit counter
	vendor_node_key := "vendor-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + jsondata["vendor_id"].(string)

	vendor_node_hit := 1
	vendor_node_hit_s, err := r.HGet(vendor_node_key, "hit").Result()
	if err == nil {
		vendor_node_hit, _ = strconv.Atoi(vendor_node_hit_s)
		vendor_node_hit = vendor_node_hit + 1

	}
	r.HSet(vendor_node_key, "hit", vendor_node_hit)

	ResponseJson(c, 200, "ok", make(map[string]string))
}

func delete(c *gin.Context) {

	r = getRedisInstance()
	_, err := r.Ping().Result()
	if err != nil {
		panic(err)
	}

	rawdata, err := c.GetRawData()
	if err != nil {
		ResponseJson(c, 400, "error", "bad request")
		return
	}

	var jsondata map[string]interface{}

	json.Unmarshal([]byte(rawdata), &jsondata)

	worker_group := jsondata["worker_group"].(string)
	worker_key := jsondata["worker_key"].(string)
	worker_role := jsondata["worker_role"].(string)

	job_info := jsondata["job"].(map[string]string)

	//set state to deleted
	job_key := "job-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"]
	r.HSet(job_key, "state", "deleted")

	//seek all task in job and delete
	task_key_pattern := "task-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"] + "-*"
	task_keys, _ := r.Keys(task_key_pattern).Result()

	for _, task_key := range task_keys {
		//only mark as delete state
		//r.HSet(task_key, "state", "deleted")

		//delete task excluding from error list
		error_task_set_key := "error_task-" + worker_group + "-" + worker_key + "-" + worker_role

		ismember, _ := r.SIsMember(error_task_set_key, task_key).Result()
		if ismember == false {
			r.HDel(task_key)
		}

	}

	ResponseJson(c, 200, "ok", make(map[string]string))
}

func done(c *gin.Context) {

	r = getRedisInstance()
	_, err := r.Ping().Result()
	if err != nil {
		panic(err)
	}

	rawdata, err := c.GetRawData()
	if err != nil {
		ResponseJson(c, 400, "error", "bad request")
		return
	}

	var jsondata map[string]interface{}

	json.Unmarshal([]byte(rawdata), &jsondata)

	worker_group := jsondata["worker_group"].(string)
	worker_key := jsondata["worker_key"].(string)
	worker_role := jsondata["worker_role"].(string)

	//seek all jobs done
	jobs_done_key := "jobs_done-" + worker_group + "-" + worker_key + "-" + worker_role
	job_keys, _ := r.SMembers(jobs_done_key).Result()

	done_job_keys := make([]string, 0)

	for _, job_key := range job_keys {
		done_job_keys = append(done_job_keys, job_key)
	}

	ResponseJson(c, 200, "ok", done_job_keys)
}

func detail(c *gin.Context) {

	r = getRedisInstance()
	_, err := r.Ping().Result()
	if err != nil {
		panic(err)
	}

	rawdata, err := c.GetRawData()
	if err != nil {
		ResponseJson(c, 400, "error", "bad request")
		return
	}

	var jsondata map[string]interface{}

	json.Unmarshal([]byte(rawdata), &jsondata)

	worker_group := jsondata["worker_group"].(string)
	worker_key := jsondata["worker_key"].(string)
	worker_role := jsondata["worker_role"].(string)

	job_info := jsondata["job"].(map[string]string)

	job_key := "job-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"]

	data := make(map[string]interface{})

	data["worker_group"], _ = r.HGet(job_key, "worker_group").Result()
	data["worker_role"], _ = r.HGet(job_key, "worker_role").Result()

	job_info["meta"], _ = r.HGet(job_key, "meta").Result()
	job_info["description"], _ = r.HGet(job_key, "description").Result()
	job_info["priority"], _ = r.HGet(job_key, "priority").Result()
	job_info["length"], _ = r.HGet(job_key, "length").Result()
	job_info["state"], _ = r.HGet(job_key, "state").Result()
	job_info["create_time"], _ = r.HGet(job_key, "create_time").Result()
	job_info["finish_time"], _ = r.HGet(job_key, "finish_time").Result()

	data["job"] = job_info

	//read result from task
	tasklist := make([]map[string]string, 0)

	task_key_pattern := "task-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"] + "-*"
	task_keys, _ := r.Keys(task_key_pattern).Result()

	for _, task_key := range task_keys {

		task_info := make(map[string]string)

		task_info["meta"], _ = r.HGet(task_key, "meta").Result()
		task_info["addressing"], _ = r.HGet(task_key, "addressing").Result()
		task_info["port"], _ = r.HGet(task_key, "port").Result()
		task_info["state"], _ = r.HGet(task_key, "state").Result()
		task_info["note"], _ = r.HGet(task_key, "note").Result()
		task_info["hash"], _ = r.HGet(task_key, "hash").Result()
		task_info["create_time"], _ = r.HGet(task_key, "create_time").Result()
		task_info["start_time"], _ = r.HGet(task_key, "start_time").Result()
		task_info["finish_time"], _ = r.HGet(task_key, "finish_time").Result()
		task_info["result"], _ = r.HGet(task_key, "result").Result()

		/*
			//read result from disk if done
			if task_info["state"] == "done" || task_info["state"] == "deleted" {
				result_filename := Filedirfromhash(task_info["hash"]) + task_info["hash"] + ".result"
				if Existfile(result_filename) == true {
					task_info["result"] = Readfile(result_filename)
				}

			}
		*/

		tasklist = append(tasklist, task_info)
	}
	data["tasks"] = tasklist

	ResponseJson(c, 200, "ok", data)
}

func read(c *gin.Context) {

	r = getRedisInstance()
	_, err := r.Ping().Result()
	if err != nil {
		panic(err)
	}

	rawdata, err := c.GetRawData()
	if err != nil {
		ResponseJson(c, 400, "error", "bad request")
		return
	}

	var jsondata map[string]interface{}

	json.Unmarshal([]byte(rawdata), &jsondata)

	worker_group := jsondata["worker_group"].(string)
	worker_key := jsondata["worker_key"].(string)
	worker_role := jsondata["worker_role"].(string)

	job_info := jsondata["job"].(map[string]string)

	//mark a job as read by remove from set
	jobs_done_key := "jobs_done-" + worker_group + "-" + worker_key + "-" + worker_role

	r.SRem(jobs_done_key, job_info["job_id"])

	ResponseJson(c, 200, "ok", make(map[string]string))
}

func retry(c *gin.Context) {

	r = getRedisInstance()
	_, err := r.Ping().Result()
	if err != nil {
		panic(err)
	}

	rawdata, err := c.GetRawData()
	if err != nil {
		ResponseJson(c, 400, "error", "bad request")
		return
	}

	var jsondata map[string]interface{}

	json.Unmarshal([]byte(rawdata), &jsondata)

	worker_group := jsondata["worker_group"].(string)
	worker_key := jsondata["worker_key"].(string)
	worker_role := jsondata["worker_role"].(string)

	job_info := jsondata["job"].(map[string]string)

	task_key_pattern := "task-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"] + "-*"
	task_keys, _ := r.Keys(task_key_pattern).Result()

	//repush to the right of list if timeout
	for _, task_key := range task_keys {
		task_state, _ := r.HGet(task_key, "state").Result()

		if task_state == "timeout" || task_state == "error" {

			priority, _ := r.HGet(task_key, "priority").Result()
			work_key := "work-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + priority

			r.RPush(work_key, task_key)

		}

		//remove from tasks_pending set
		tasks_pending_key := "tasks_pending-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"]
		r.SRem(tasks_pending_key, task_key)

		//added to tasks_waiting set
		tasks_waiting_key := "tasks_waiting-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"]
		r.SAdd(tasks_waiting_key, task_key)

	}

	ResponseJson(c, 200, "ok", make(map[string]string))
}
