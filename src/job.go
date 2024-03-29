package main

//api for worker

import (
	"encoding/json"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-redis/redis"
)

func jobping(c *gin.Context) {

	rawdata, err := c.GetRawData()
	if err != nil {
		ResponseJson(c, 400, "error", "bad request")
		return
	}

	r = getRedisInstance()

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

	//add to vendor set
	vendor_set := "vendor_set-" + worker_group + "-" + worker_key + "-" + worker_role
	r.SAdd(vendor_set, vendor_node_key)

	//add to vendor all set
	vendor_set_all := "vendor_set_all"
	r.SAdd(vendor_set_all, vendor_node_key)

	data := "pong"

	ResponseJson(c, 200, "ok", data)

}

func assign(c *gin.Context) {

	rawdata, err := c.GetRawData()
	if err != nil {
		ResponseJson(c, 400, "error", "bad request")
		return
	}

	r = getRedisInstance()

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
	r.HSet(job_key, "priority", job_info["priority"].(float64))

	tasks := jsondata["tasks"].([]interface{})

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

	//statistics set
	statistics_task_addressing_key_set := "statistics_task_addressing_set-" + worker_group + "-" + worker_key + "-" + worker_role

	statistics_task_port_key_set := "statistics_task_port_set-" + worker_group + "-" + worker_key + "-" + worker_role

	//---------------------------------
	//job set of the worker role
	job_set := "job_set-" + worker_group + "-" + worker_key + "-" + worker_role

	//add to the role's job set
	job_member := redis.Z{
		Score:  float64(time.Now().Unix()),
		Member: job_key,
	}
	r.ZAdd(job_set, job_member)

	//task set of the job
	task_set := "task_set-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"].(string)

	task_index := 0

	//save task records
	ignore_count := int64(0)

	//use redis pipeline
	p := r.Pipeline()

	for index, _ := range tasks {

		task_info := tasks[index].(map[string]interface{})
		//make task records

		task_info["hash"] = Md5(task_info["data"].(string))
		task_info["index"] = strconv.Itoa(task_index)

		task_key := "task-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"].(string) + "-" + task_info["index"].(string)

		p.HSet(task_key, "state", "waiting")
		p.HSet(task_key, "note", "")
		p.HSet(task_key, "result", "")

		// add create timestamp
		p.HSet(task_key, "create_time", time.Now().Unix())
		p.HSet(task_key, "start_time", "")
		p.HSet(task_key, "finish_time", "")
		p.HSet(task_key, "try_times", 0)

		p.HSet(task_key, "job_id", job_info["job_id"])
		p.HSet(task_key, "priority", job_info["priority"])

		p.HSet(task_key, "hash", task_info["hash"])
		p.HSet(task_key, "index", task_info["index"])

		p.HSet(task_key, "meta", task_info["meta"])
		p.HSet(task_key, "addressing", task_info["addressing"])
		p.HSet(task_key, "port", task_info["port"])
		p.HSet(task_key, "timeout", task_info["timeout"])
		p.HSet(task_key, "try_times_limit", task_info["try_times_limit"])

		//add to the job's task set
		task_index = task_index + 1
		task_member := redis.Z{
			Score:  float64(task_index),
			Member: task_key,
		}
		p.ZAdd(task_set, task_member)

		//task addressing and port count statistics
		statistics_task_addressing_key := statistics_task_addressing_key_base + "-" + task_info["addressing"].(string)
		p.IncrBy(statistics_task_addressing_key, 1)

		statistics_task_port_key := statistics_task_port_key_base + "-" + task_info["port"].(string)
		p.IncrBy(statistics_task_port_key, 1)

		//add to statistics set
		p.SAdd(statistics_task_addressing_key_set, statistics_task_addressing_key)
		p.SAdd(statistics_task_port_key_set, statistics_task_port_key)

		//skip ignore task
		if task_info["port"] == "ignore" {
			ignore_count = ignore_count + 1
			p.HSet(task_key, "state", "done")
			p.HSet(task_key, "note", "ignore file")
			p.HSet(task_key, "finish_time", time.Now().Unix())
			p.HSet(task_key, "result", "")
			if ignore_count == length {
				//add a job to set as unread
				jobs_done_key := "jobs_done-" + worker_group + "-" + worker_key + "-" + worker_role

				p.SAdd(jobs_done_key, task_info["job_id"])

				//add finish timestamp
				p.HSet(job_key, "finish_time", time.Now().Unix())
				p.HSet(job_key, "state", "done")

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

		//commit redis pipeline
		p.Exec()

		//save binary to disk for tmp use
		if task_info["addressing"] == "binary" {
			taskdata_filename := Filedirfromhash(task_info["hash"].(string)) + task_info["hash"].(string) + ".taskdata"
			Makedirforhash(task_info["hash"].(string))
			Writefile(taskdata_filename, task_info["data"].(string), "w")
			task_info["data"] = ""
		} else {
			r.HSet(task_key, "data", task_info["data"])
		}

		//allocate task to work priority list
		work_key := "work-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + strconv.FormatFloat(job_info["priority"].(float64), 'f', -1, 64)
		r.LPush(work_key, task_key)

		//add task to tasks_waiting to wait for job state check
		tasks_waiting_key := "tasks_waiting-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"].(string)
		r.SAdd(tasks_waiting_key, task_key)

	}

	//add to priority set
	priority_set := "priority_set-" + worker_group + "-" + worker_key + "-" + worker_role
	priority_member := redis.Z{
		Score:  job_info["priority"].(float64),
		Member: job_info["priority"].(float64),
	}
	r.ZAdd(priority_set, priority_member)

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

	rawdata, err := c.GetRawData()
	if err != nil {
		ResponseJson(c, 400, "error", "bad request")
		return
	}

	r = getRedisInstance()

	var jsondata map[string]interface{}

	json.Unmarshal([]byte(rawdata), &jsondata)

	worker_group := jsondata["worker_group"].(string)
	worker_key := jsondata["worker_key"].(string)
	worker_role := jsondata["worker_role"].(string)

	job_info := jsondata["job"].(map[string]interface{})

	//set state to deleted
	job_key := "job-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"].(string)
	r.HSet(job_key, "state", "deleted")

	//set deleted job ttl
	timeout_ttl, _ := strconv.Atoi(Error_TTL)
	r.Expire(job_key, time.Second*time.Duration(timeout_ttl))

	//del the role's job set
	//job_set := "job_set-" + worker_group + "-" + worker_key + "-" + worker_role
	//r.ZRem(job_set, job_key)

	//seek all task in job and delete
	//task set of the job
	task_set := "task_set-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"].(string)

	task_keys, _ := r.ZRange(task_set, 0, -1).Result()

	//use redis pipeline
	p := r.Pipeline()

	for _, task_key := range task_keys {
		//only mark as delete state
		//r.HSet(task_key, "state", "deleted")

		//delete from task_set
		p.ZRem(task_set, task_key)

		//delete task excluding from error list
		error_task_set_key := "error_task-" + worker_group + "-" + worker_key + "-" + worker_role

		_, err := r.ZRank(error_task_set_key, task_key).Result()
		if err == nil {
			p.Del(task_key)
		}

	}

	//commit redis pipeline
	p.Exec()

	ResponseJson(c, 200, "ok", make(map[string]string))
}

func done(c *gin.Context) {

	rawdata, err := c.GetRawData()
	if err != nil {
		ResponseJson(c, 400, "error", "bad request")
		return
	}

	r = getRedisInstance()

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

	data := make(map[string]interface{})

	data["done"] = done_job_keys

	ResponseJson(c, 200, "ok", data)
}

func detail(c *gin.Context) {

	rawdata, err := c.GetRawData()
	if err != nil {
		ResponseJson(c, 400, "error", "bad request")
		return
	}

	r = getRedisInstance()

	var jsondata map[string]interface{}

	json.Unmarshal([]byte(rawdata), &jsondata)

	worker_group := jsondata["worker_group"].(string)
	worker_key := jsondata["worker_key"].(string)
	worker_role := jsondata["worker_role"].(string)

	job_info := jsondata["job"].(map[string]interface{})

	job_key := "job-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"].(string)

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

	//task set of the job
	task_set := "task_set-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"].(string)

	task_keys, _ := r.ZRange(task_set, 0, -1).Result()

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

	//mark a job as read by remove from set
	jobs_done_key := "jobs_done-" + worker_group + "-" + worker_key + "-" + worker_role

	r.SRem(jobs_done_key, job_info["job_id"])

	ResponseJson(c, 200, "ok", make(map[string]string))
}

func retry(c *gin.Context) {

	rawdata, err := c.GetRawData()
	if err != nil {
		ResponseJson(c, 400, "error", "bad request")
		return
	}

	r = getRedisInstance()

	var jsondata map[string]interface{}

	json.Unmarshal([]byte(rawdata), &jsondata)

	worker_group := jsondata["worker_group"].(string)
	worker_key := jsondata["worker_key"].(string)
	worker_role := jsondata["worker_role"].(string)

	job_info := jsondata["job"].(map[string]interface{})

	//task set of the job
	task_set := "task_set-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"].(string)

	task_keys, _ := r.ZRange(task_set, 0, -1).Result()

	//use redis pipeline
	p := r.Pipeline()

	//repush to the right of list if timeout
	for _, task_key := range task_keys {
		task_state, _ := r.HGet(task_key, "state").Result()

		if task_state == "timeout" || task_state == "error" {

			priority, _ := r.HGet(task_key, "priority").Result()
			work_key := "work-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + priority

			p.RPush(work_key, task_key)

		}

		//remove from tasks_pending
		tasks_pending_key := "tasks_pending-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"].(string)
		p.SRem(tasks_pending_key, task_key)

		//remove from tasks_pending total set
		tasks_pending_set := "tasks_pending-" + worker_group + "-" + worker_key + "-" + worker_role
		p.SRem(tasks_pending_set, task_key)

		//remove from tasks_pending all set
		tasks_pending_all := "tasks_pending_all"
		p.SRem(tasks_pending_all, task_key)

		//added to tasks_waiting set
		tasks_waiting_key := "tasks_waiting-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_info["job_id"].(string)
		p.SAdd(tasks_waiting_key, task_key)

	}

	//commit redis pipeline
	p.Exec()

	ResponseJson(c, 200, "ok", make(map[string]string))
}
