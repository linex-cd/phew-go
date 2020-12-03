package main

//api for monitor

import "time"


import "github.com/gin-gonic/gin"

import "github.com/shirou/gopsutil/cpu"
import "github.com/shirou/gopsutil/mem"
import "github.com/shirou/gopsutil/disk"

import "github.com/go-redis/redis"

import "container/list"

var r *redis.Client

func init(){
	r := redis.NewClient(&redis.Options{
		Addr:		Redis_host,
		Password:	"",
		DB:			0,
	})
	

}



func system(c *gin.Context){
	


		//REDIS DB  1G
		db := int(Filesize(Data_dir + "redis.rdb") / (1024*1024*1024*1) * 100)

		
		//CPU
		percent, _ := cpu.Percent(time.Second, false)
		cpu := int(percent[0])
		
		//memory
		memInfo, _ := mem.VirtualMemory()
		memory :=  int(memInfo.UsedPercent)
		
		//systemdisk
		sysdiskInfo, _ := disk.Usage(System_disk)
		systemdisk :=int(sysdiskInfo.UsedPercent) 
		
		//datadisk
		datadiskInfo, _ := disk.Usage(Data_disk)
		datadisk := int(datadiskInfo.UsedPercent)

		//GPU
		gpu := 90
		
		//temp
		temp := 60
		
			
		data  := map[string]int{
					"db": db,
					"cpu": cpu,
					"memory": memory,
					"gpu": gpu,
					"systemdisk": systemdisk,
					"datadisk": datadisk,
					"temp": temp,
				}
		
	
	ResponseJson(c, 200, "ok", data)
	
}



func latestwork(request string) {
	
		
	pong, err := r.Ping().Result()
	if err != nil {
		panic(err)
	}
	job_total_pattern := "job-*"
	jobs := r.Keys(job_total_pattern).Result()

	job_latest := list.New()
	for job in jobs {
	
		job_key = job.decode()
		
		//latest
		length = int(r.hget(job_key, "length").decode())
		priority = int(r.hget(job_key, "priority").decode())
		description = r.hget(job_key, "description").decode()
		job_id = job_key.split("-")[-1]
		create_time = r.hget(job_key, "create_time").decode()
		item = (create_time, length, job_id, priority, description, encrypt(job_key))
		job_latest.PushBack(item)
		
	}
	
	//job_latest sort by create_time
	job_latest = sorted(job_latest, key=lambda x: (x[0]))
	job_latest = job_latest[-20:]


	//task_latest
	task_latest = []
	tasks_pending_pattern = "tasks_pending-*"
	tasks_pending_set_keys = r.keys(tasks_pending_pattern)
	for tasks_pending_set_key in tasks_pending_set_keys {
	
		job_key = "job-" + tasks_pending_set_key.decode()[14:]
		
		tasks = r.smembers(tasks_pending_set_key)
		tasks = list(tasks)
		for task_key in tasks {
			task_key = task_key.decode()
			if  r.hget(job_key, "description") == None:
				r.srem(tasks_pending_set_key, task_key)
				continue
			item = {}
			item["job_id"] = job_key.split("-")[-1]

			item["description"] = r.hget(job_key, "description").decode()
			
			start_time = r.hget(task_key, "start_time")
			if start_time == None:
				//ignore deleted task
				continue
			#endif
			
			addressing = r.hget(task_key, "addressing").decode()
			if  addressing == "binary":
				item["data"] = "BINARY"
			else:
				item["data"] = r.hget(task_key, "data").decode()
			#endif
			
			item["port"] = r.hget(task_key, "port").decode()
			item["addressing"] = addressing
			item["create_time"] = int(r.hget(task_key, "create_time").decode())
			
			item["job_access_key"] = encrypt(job_key)
			item["task_access_key"] = encrypt(task_key)
			task_latest.append(item)
		}
	}

	//task_latest sort by create_time
	task_latest = sorted(task_latest, key=lambda x: (x["create_time"]))
	task_latest = task_latest[-20:]
	
	
	data  = {

				"job_latest": job_latest,
				"task_latest": task_latest,
			}
	

	return response(200, "ok", data)

}
