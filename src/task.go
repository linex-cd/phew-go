package main

//api for worker

import (
	"encoding/json"
	"sort"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

func taskping(c *gin.Context) {

	r = getRedisInstance()

	rawdata, err := c.GetRawData()
	if err != nil {
		ResponseJson(c, 400, "error", "bad request")
		return
	}

	var jsondata map[string]interface{}

	json.Unmarshal([]byte(rawdata), &jsondata)

	worker_id := jsondata["worker_id"].(string)

	worker_group := jsondata["worker_group"].(string)
	worker_key := jsondata["worker_key"].(string)
	worker_role := jsondata["worker_role"].(string)

	worker_node_key := "worker-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + worker_id

	worker_node_ip := c.ClientIP()

	//set to online
	r.HSet(worker_node_key, "ping_time", time.Now().Unix())
	r.HSet(worker_node_key, "ip", worker_node_ip)

	r.HSet(worker_node_key, "worker_id", jsondata["worker_id"].(string))
	r.HSet(worker_node_key, "name", jsondata["worker_name"].(string))
	r.HSet(worker_node_key, "location", jsondata["worker_location"].(string))
	r.HSet(worker_node_key, "state", "online")

	//add to worker set
	worker_set := "worker_set-" + worker_group + "-" + worker_key + "-" + worker_role
	r.SAdd(worker_set, worker_node_key)

	data := "pong"

	ResponseJson(c, 200, "ok", data)

}

func get(c *gin.Context) {

	r = getRedisInstance()

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

	//get all work list keys
	work_key_pattern := "work-" + worker_group + "-" + worker_key + "-" + worker_role + "-*"
	work_keys, _ := r.Keys(work_key_pattern).Result()

	if len(work_keys) == 0 {
		ResponseJson(c, 404, "no task", make(map[string]string))
		return
	}

	//sort and get the highest priority key
	sort.Strings(work_keys)
	work_key := work_keys[len(work_keys)-1]

	//popup a valid task_key and get the task info
	task_info := make(map[string]string)

	var task_key string
	for {

		task_key_tmp, err := r.RPop(work_key).Result()
		if err != nil {
			ResponseJson(c, 404, "no task", make(map[string]string))
			return
		}

		task_key = task_key_tmp

		job_id, err := r.HGet(task_key, "job_id").Result()
		if err != nil {
			continue
		}

		task_info["job_id"] = job_id
		task_info["priority"], _ = r.HGet(task_key, "priority").Result()
		task_info["meta"], _ = r.HGet(task_key, "meta").Result()
		task_info["addressing"], _ = r.HGet(task_key, "addressing").Result()
		task_info["port"], _ = r.HGet(task_key, "port").Result()
		task_info["hash"], _ = r.HGet(task_key, "hash").Result()

		//add start timestamp
		r.HSet(task_key, "start_time", time.Now().Unix())

		//read data from disk if done
		if task_info["addressing"] == "binary" {

			taskdata_filename := Filedirfromhash(task_info["hash"]) + task_info["hash"] + ".taskdata"

			if Existfile(taskdata_filename) == true {
				taskdata := Readfile(taskdata_filename)
				task_info["data"] = taskdata
				r.HSet(task_key, "state", "waiting")
				break
			} else {
				//mark task as error and then repop a new task
				r.HSet(task_key, "state", "error")

				// add finish timestamp
				r.HSet(task_key, "finish_time", time.Now().Unix())

				continue
			}
		} else {
			task_info["data"], _ = r.HGet(task_key, "data").Result()
			r.HSet(task_key, "state", "waiting")

			break
		}

	}

	//added to tasks_pending set
	tasks_pending_key := "tasks_pending-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + task_info["job_id"]
	r.SAdd(tasks_pending_key, task_key)

	//remove from tasks_waiting set
	tasks_waiting_key := "tasks_waiting-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + task_info["job_id"]
	r.SRem(tasks_waiting_key, task_key)

	//update worker node hit counter
	worker_id := jsondata["worker_id"].(string)
	worker_node_key := "worker-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + worker_id

	worker_node_hit_s, err := r.HGet(worker_node_key, "hit").Result()
	worker_node_hit := 1
	if err != nil {
		worker_node_hit_s = "1"
	} else {
		worker_node_hit, _ := strconv.Atoi(worker_node_hit_s)
		worker_node_hit = worker_node_hit + 1
	}

	r.HSet(worker_node_key, "hit", worker_node_hit)

	ResponseJson(c, 200, "ok", task_info)
}

func finish(c *gin.Context) {

	r = getRedisInstance()

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

	//update task result and state
	task_info := jsondata["task"].(map[string]interface{})

	job_id := task_info["job_id"].(string)
	hash := task_info["hash"].(string)

	job_key := "job-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_id

	task_key := "task-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_id + "-" + hash

	old_task_state, err := r.HGet(task_key, "state").Result()
	if err == nil {
		//keep deleted state
		if old_task_state == "deleted" {
			task_info["state"] = old_task_state
		}
	} else {
		//already deleted
		ResponseJson(c, 200, "ok", make(map[string]string))
		return
	}

	// add finish timestamp
	r.HSet(task_key, "finish_time", time.Now().Unix())

	r.HSet(task_key, "state", task_info["state"])
	r.HSet(task_key, "note", task_info["note"])
	r.HSet(task_key, "result", task_info["result"])

	//remove from tasks_pending set
	tasks_pending_key := "tasks_pending-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_id
	r.SRem(tasks_pending_key, task_key)

	tasks_waiting_key := "tasks_waiting-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_id

	//delete binary tmp file
	if task_info["addressing"] == "binary" {
		taskdata_filename := Filedirfromhash(task_info["hash"].(string)) + task_info["hash"].(string) + ".taskdata"
		if Existfile(taskdata_filename) == true {
			Removefile(taskdata_filename)
		}
	}

	//check if tasks_waiting and tasks_pending set are empty
	tasks_waiting_key_count, _ := r.SCard(tasks_waiting_key).Result()
	tasks_pending_key_count, _ := r.SCard(tasks_pending_key).Result()

	if tasks_waiting_key_count == 0 && tasks_pending_key_count == 0 {
		//add a job to set as unread
		jobs_done_key := "jobs_done-" + worker_group + "-" + worker_key + "-" + worker_role

		r.SAdd(jobs_done_key, task_info["job_id"])

		//add finish timestamp
		r.HSet(job_key, "finish_time", time.Now().Unix())
		r.HSet(job_key, "state", "done")

		//job pending statistics
		statistics_job_pending_key := "statistics_job_pending-" + worker_group + "-" + worker_key + "-" + worker_role
		r.DecrBy(statistics_job_pending_key, 1)

		statistics_job_pending_count_s, _ := r.Get(statistics_job_pending_key).Result()
		statistics_job_pending_count_i, _ := strconv.Atoi(statistics_job_pending_count_s)
		if statistics_job_pending_count_i < 0 {
			r.Set(statistics_job_pending_key, "0", -1)
		}

	}

	ResponseJson(c, 200, "ok", make(map[string]string))
}
