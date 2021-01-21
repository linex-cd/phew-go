package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis"
)

//daemon for timeout task

//timeout = 60, try_times_limit = 3

func daemon_thread() {

	r = getRedisInstance()

	fmt.Println("started deamon thread")

	for {
		time.Sleep(30)

		//seek all tasks pending
		tasks_pending_all := "tasks_pending_all"

		tasks_pending, _ := r.SMembers(tasks_pending_all).Result()

		for _, task_key := range tasks_pending {

			//remove from pending set if timeout and mark timeout

			job_id, err := r.HGet(task_key, "job_id").Result()
			if err != nil {
				fmt.Println("some key dismissed, continue")
				continue
			}

			task_create_time, err := r.HGet(task_key, "start_time").Result()
			if err != nil {
				fmt.Println("some key dismissed, continue")
				continue
			}

			task_timeout, err := r.HGet(task_key, "timeout").Result()
			if err != nil {
				fmt.Println("some key dismissed, continue")
				continue
			}

			task_try_times_limit, err := r.HGet(task_key, "try_times_limit").Result()
			if err != nil {
				fmt.Println("some key dismissed, continue")
				continue
			}

			worker_tmp := strings.Split(task_key, "-")
			worker_group := worker_tmp[1]
			worker_key := worker_tmp[2]
			worker_role := worker_tmp[3]
			//job_id = worker_tmp[4]

			job_key := "job-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_id

			tasks_pending_key := "tasks_waiting-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + job_id
			tasks_waiting_key := strings.Replace(tasks_pending_key, "tasks_pending-", "tasks_waiting-", -1)

			task_create_time_i32, err := strconv.Atoi(task_create_time)
			task_create_time_i := int64(task_create_time_i32)

			task_timeout_i32, err := strconv.Atoi(task_timeout)
			task_timeout_i := int64(task_timeout_i32)

			task_try_times_limit_i32, err := strconv.Atoi(task_try_times_limit)

			if time.Now().Unix()-task_create_time_i > task_timeout_i {

				fmt.Println("found a task timeout:", task_key)

				//try_times under limit
				try_times, err := r.HGet(task_key, "try_times").Result()
				if err != nil {
					fmt.Println("task_key dismissed, continue")
					continue
				}

				try_times_i, err := strconv.Atoi(try_times)

				if try_times_i < task_try_times_limit_i32 {
					fmt.Println("resend task to work list:", task_key)

					//increase try_times
					r.HSet(task_key, "try_times", try_times_i+1)

					//reset state
					r.HSet(task_key, "state", "waiting")

					//remove from pending set
					r.SRem(tasks_pending_key, task_key)

					//remove from all pending set
					r.SRem(tasks_pending_all, task_key)

					//add to waiting set
					r.SAdd(tasks_waiting_key, task_key)

					//push to work list
					priority, err := r.HGet(task_key, "priority").Result()
					if err != nil {
						fmt.Println("task_key dismissed, continue")
						continue
					}

					work_key := "work-" + worker_group + "-" + worker_key + "-" + worker_role + "-" + priority
					r.RPush(work_key, task_key)

					//add to priority set
					priority_set := "priority_set-" + worker_group + "-" + worker_key + "-" + worker_role
					priority_i, _ := strconv.Atoi(priority)
					priority_member := redis.Z{
						Score:  float64(priority_i),
						Member: float64(priority_i),
					}
					r.ZAdd(priority_set, priority_member)

				} else {
					fmt.Println("mark task timeout:", task_key)

					//mark state timeout
					r.HSet(task_key, "state", "timeout")

					//remove from  pending set
					r.SRem(tasks_pending_key, task_key)

					//remove from all pending set
					r.SRem(tasks_pending_all, task_key)

					//send to error set
					error_job_set_key := "error_job-" + worker_group + "-" + worker_key + "-" + worker_role
					error_job_member := redis.Z{
						Score:  float64(time.Now().Unix()),
						Member: job_key,
					}
					r.ZAdd(error_job_set_key, error_job_member)

					error_task_set_key := "error_task-" + worker_group + "-" + worker_key + "-" + worker_role
					error_task_member := redis.Z{
						Score:  float64(time.Now().Unix()),
						Member: task_key,
					}
					r.ZAdd(error_task_set_key, error_task_member)

					//if last one task is timeout, the mark the job as done
					//check if tasks_waiting and tasks_pending set are empty
					tasks_waiting_key_count, err := r.SCard(tasks_waiting_key).Result()
					if err != nil {
						fmt.Println("tasks_waiting_key dismissed, continue")
						continue
					}
					tasks_pending_key_count, err := r.SCard(tasks_pending_key).Result()
					if err != nil {
						fmt.Println("tasks_pending_key dismissed, continue")
						continue
					}

					if tasks_waiting_key_count == 0 && tasks_pending_key_count == 0 {
						//add a job to set as unread
						jobs_done_key := "jobs_done-" + worker_group + "-" + worker_key + "-" + worker_role

						r.SAdd(jobs_done_key, job_id)

						//add finish timestamp
						r.HSet(job_key, "finish_time", time.Now().Unix())
						r.HSet(job_key, "state", "done")

						statistics_job_pending_key := "statistics_job_pending-" + worker_group + "-" + worker_key + "-" + worker_role
						r.DecrBy(statistics_job_pending_key, 1)

						statistics_job_pending_key_count, err := r.Get(statistics_job_pending_key).Result()
						if err != nil {
							fmt.Println("statistics_job_pending_key dismissed, continue")
							continue
						}

						statistics_job_pending_key_count_i, _ := strconv.Atoi(statistics_job_pending_key_count)
						if statistics_job_pending_key_count_i < 0 {
							r.Set(statistics_job_pending_key, 0, -1)
						}

					}
				}

			}

		}

	}
}
