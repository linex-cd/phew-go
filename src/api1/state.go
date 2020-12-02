// api for monitor

import "github.com/shirou/gopsutil"
import "github.com/go-redis/redis"

r := redis.NewClient(&redis.Options{
	Addr:		Redis_host,
	Password:	"",
	DB:			0,
})



def index(request):
	return redirect("/index.html")


def sysstate(request):
	

	if request.method == 'GET':
		
		#REDIS DB  1G
		db := Filesize(Data_dir + 'redis.rdb') / (1024*1024*1024*1) * 100)

		
		#CPU
		cpus, _:= cpu.Percent(time.Second, false)
		cpu := int(cpu[0])
		
		#memory
		memInfo, _ := mem.VirtualMemory()
		memory :=  int(memInfo.UsedPercent)
		
		#systemdisk
		diskInfo, _ := disk.Usage(System_disk)
		systemdisk :=int(diskInfo.UsedPercent) 
		
		#datadisk
		diskInfo, _ := disk.Usage(Data_disk)
		datadisk := int(diskInfo.UsedPercent)

		#GPU
		gpu := 90
		
		#temp
		temp := 60
		
			
		data  = {
					'db': db,
					'cpu': cpu,
					'memory': memory,
					'gpu': gpu,
					'systemdisk': systemdisk,
					'datadisk': datadisk,
					'temp': temp,
				}
		

	return response(200, "ok", data)

def latestwork(request):
	if request.method == 'GET':
		
		pong, err := client.Ping().Result()
		if err != nil {
			panic(err)
		}
		job_total_pattern = 'job-*'
		jobs = r.keys(job_total_pattern)

		job_latest = []
		for job in jobs:
		
			job_key = job.decode()
			
			#latest
			length = int(r.hget(job_key, 'length').decode())
			priority = int(r.hget(job_key, 'priority').decode())
			description = r.hget(job_key, 'description').decode()
			job_id = job_key.split("-")[-1]
			create_time = r.hget(job_key, 'create_time').decode()
			item = (create_time, length, job_id, priority, description, encrypt(job_key))
			job_latest.append(item)
			
		#endfor
		
		#job_latest sort by create_time
		job_latest = sorted(job_latest, key=lambda x: (x[0]))
		job_latest = job_latest[-20:]

	
		#task_latest
		task_latest = []
		tasks_pending_pattern = 'tasks_pending-*'
		tasks_pending_set_keys = r.keys(tasks_pending_pattern)
		for tasks_pending_set_key in tasks_pending_set_keys:
		
			job_key = 'job-' + tasks_pending_set_key.decode()[14:]
			
			tasks = r.smembers(tasks_pending_set_key)
			tasks = list(tasks)
			for task_key in tasks:
				task_key = task_key.decode()
				if  r.hget(job_key, 'description') == None:
					r.srem(tasks_pending_set_key, task_key)
					continue
				item = {}
				item['job_id'] = job_key.split("-")[-1]

				item['description'] = r.hget(job_key, 'description').decode()
				
				start_time = r.hget(task_key, 'start_time')
				if start_time == None:
					#ignore deleted task
					continue
				#endif
				
				addressing = r.hget(task_key, 'addressing').decode()
				if  addressing == "binary":
					item['data'] = 'BINARY'
				else:
					item['data'] = r.hget(task_key, 'data').decode()
				#endif
				
				item['port'] = r.hget(task_key, 'port').decode()
				item['addressing'] = addressing
				item['create_time'] = int(r.hget(task_key, 'create_time').decode())
				
				item['job_access_key'] = encrypt(job_key)
				item['task_access_key'] = encrypt(task_key)
				task_latest.append(item)
			#endfor
		#endfor
		#task_latest sort by create_time
		task_latest = sorted(task_latest, key=lambda x: (x['create_time']))
		task_latest = task_latest[-20:]
		
		
		data  = {

					'job_latest': job_latest,
					'task_latest': task_latest,
				}
		

	return response(200, "ok", data)


def jobcounter(request):
	

	if request.method == 'GET':
	
		#job_total
		statistics_job_total_pattern = 'statistics_job_total-*'
		statistics_job_total_pending_keys = r.keys(statistics_job_total_pattern)
		
		job_total = 0
		for statistics_job_total_pending_key in statistics_job_total_pending_keys:
			job_total = job_total + int(r.get(statistics_job_total_pending_key).decode())
		#endfor
		
		#task_total
		statistics_task_total_pattern = 'statistics_task_total-*'
		statistics_task_total_pending_keys = r.keys(statistics_task_total_pattern)
		
		task_total = 0
		for statistics_task_total_pending_key in statistics_task_total_pending_keys:
			task_total = task_total + int(r.get(statistics_task_total_pending_key).decode())
		#endfor
		
		#job_pending
		statistics_job_pending_pattern = 'statistics_job_pending-*'
		statistics_job_pending_pending_keys = r.keys(statistics_job_pending_pattern)
		
		job_pending = 0
		for statistics_job_pending_pending_key in statistics_job_pending_pending_keys:
			job_pending = job_pending + int(r.get(statistics_job_pending_pending_key).decode())
		#endfor
		

		#work_pending
		work_pattern = 'work-*'
		work_pending_keys = r.keys(work_pattern)
		
		work_pending = 0
		for work_pending_key in work_pending_keys:
			work_pending = work_pending + r.llen(work_pending_key)
		#endfor
		

		data  = {
					'job_total': job_total,
					'task_total': task_total,
					'job_pending': job_pending,
					'work_pending': work_pending,
				}
		

	return response(200, "ok", data)


#-------------------------------------------------------------------------------

def inlist(request):
	

	if request.method == 'GET':
		
		job_total_pattern = 'job-*'
		jobs = r.keys(job_total_pattern)
		job_total = len(jobs)
		
		
		job_pending = 0
		task_total = 0
		
		jobs_list = []
		for job in jobs:
		
			job_key = job.decode()
			
			#job_pending
			state = r.hget(job_key, 'state').decode()
			if state == 'assigned':
				job_pending = job_pending + 1
			#endif
			
			#task_total
			length = int(r.hget(job_key, 'length').decode())
			task_total = task_total + length
			
			#latest
			description = r.hget(job_key, 'description').decode()
			job_id = job_key.split("-")[-1]
			create_time = r.hget(job_key, 'create_time').decode()
			item = (create_time, length, job_id, description, encrypt(job_key))
			jobs_list.append(item)
			
		#endfor
		
		
		#job_latest sort by create_time
		jobs_list = sorted(jobs_list, key=lambda x: (x[0]))
		
		#work in list
		work_pattern = 'work-*'
		works = r.keys(work_pattern)
		
		works_list = []
		for task_key in works:
			job_key = 'job-' + task_key.decode()[5:-33]
			item = {}
			item['job_id'] = job_key.split("-")[-1]
			item['description'] =  r.hget(job_key, 'description').decode()
			
			item['port'] =  r.hget(task_key, 'port').decode()
			item['data'] =  r.hget(job_key, 'data').decode()
			
			addressing = r.hget(task_key, 'addressing').decode()
			if  addressing == "binary":
				item['data'] = 'BINARY'
			else:
				item['data'] = r.hget(task_key, 'data').decode()
			#endif
			
			item['port'] = r.hget(task_key, 'port').decode()
			item['addressing'] = addressing
			
			item['job_access_key'] = encrypt(job_key)
			item['task_access_key'] = encrypt(task_key)
				
			works_list.append(item)
		#endfor
		
		
		
		data  = {
					'jobs_list': jobs_list,
					'works_list': works_list,
				}
		

	return response(200, "ok", data)


#-------------------------------------------------------------------------------

def nodecounter(request):
	

	if request.method == 'GET':
		
		vendor_pattern = 'vendor-*'
		vendor_keys = r.keys(vendor_pattern)
		vendor_count = len(vendor_keys)
		vendors = []
		for vendor_key in vendor_keys:
			item = {}
			itemkeys = r.hgetall(vendor_key)
			for itemkey in itemkeys:
				item[itemkey.decode()] = r.hget(vendor_key, itemkey).decode()
			#endfor
			vendors.append(item)
		#endfor
		
		
		worker_pattern = 'worker-*'
		worker_keys = r.keys(worker_pattern)
		worker_count = len(worker_keys)
		workers = []
		for worker_key in worker_keys:
			item = {}
			itemkeys = r.hgetall(worker_key)
			for itemkey in itemkeys:
				item[itemkey.decode()] = r.hget(worker_key, itemkey).decode()
			#endfor
			workers.append(item)
		#endfor
		
		
		data  = {
					'vendor_count': vendor_count,
					'worker_count': worker_count,
					'vendors': vendors,
					'workers': workers,
				}
		

	return response(200, "ok", data)


#-------------------------------------------------------------------------------

def peekjob(request):
	

	if request.method == 'GET':
		
		job_access_key = request.GET.get('job_access_key', 'nokey')
		
		if job_access_key == 'nokey':
			return response(403, "forbidden", [])
		#endif
		
		job_key = decrypt(job_access_key)

		if r.hget(job_key, 'state') is None:
			return response(404, "job not found", [])
		#endif
		
		data = {}
		
		data['job_id'] = job_key.split("-")[-1]
		
		data['state'] = r.hget(job_key, 'state').decode()

		data['create_time'] = r.hget(job_key, 'create_time').decode()
		data['finish_time'] = r.hget(job_key, 'finish_time').decode()
		
		data['vendor_id'] = r.hget(job_key, 'vendor_id').decode()
		data['worker_group'] = r.hget(job_key, 'worker_group').decode()

		
		data['meta'] = r.hget(job_key, 'meta').decode()
		data['description'] = r.hget(job_key, 'description').decode()
		data['priority'] = r.hget(job_key, 'priority').decode()
	
		data['length'] = r.hget(job_key, 'length').decode()
		


	return response(200, "ok", data)


def peektask(request):
	

	if request.method == 'GET':
		
		task_access_key = request.GET.get('task_access_key', 'nokey')
		if task_access_key == 'nokey':
			return response(403, "forbidden", [])
		#endif
		
		task_key = decrypt(task_access_key)

		if r.hget(task_key, 'state') is None:
			return response(404, "task not found", [])
		#endif
		
		data = {}
		
		data['state'] = r.hget(task_key, 'state').decode()
		data['note'] = r.hget(task_key, 'note').decode()
		#data['result'] = r.hget(task_key, 'result').decode()

		data['create_time'] = r.hget(task_key, 'create_time').decode()
		data['start_time'] = r.hget(task_key, 'start_time').decode()
		data['finish_time'] = r.hget(task_key, 'finish_time').decode()

		data['job_id'] = r.hget(task_key, 'job_id').decode()
		data['priority'] = r.hget(task_key, 'priority').decode()

		data['meta'] = r.hget(task_key, 'meta').decode()
		data['addressing'] = r.hget(task_key, 'addressing').decode()
		data['port'] = r.hget(task_key, 'port').decode()
		

	return response(200, "ok", data)


from app.config import URI_dir 

def peekfile(request):
	

	if request.method == 'GET':
		
		filename = request.GET.get('filename', '/nofile')
		
		#只访问特定目录
		if filename.find(URI_dir) != 0 :
			return redirect("/index.html")
		#endif
		
		#禁止伪造目录
		if filename.find('/../') >=0 :
			return redirect("/index.html")
		#endif
		
		if existfile(filename) == False:
			return redirect("/index.html")
		#endif
		

		return responsefile(filename)
	
	return redirect("/index.html")


#-------------------------------------------------------------------------------

def percentage(request):
	

	if request.method == 'GET':
		
		addressing_data = {}
		
		#addressing_count
		statistics_task_addressing_pattern = 'statistics_task_addressing-*'
		statistics_task_addressing_keys = r.keys(statistics_task_addressing_pattern)
		
		for statistics_task_addressing_key in statistics_task_addressing_keys:
			statistics_task_addressing_key = statistics_task_addressing_key.decode()
			
			addressing = statistics_task_addressing_key.split("-")[-1]
			addressing_count = int(r.get(statistics_task_addressing_key).decode())
			
			if addressing not in addressing_data:
				addressing_data[addressing] = 0
			#endif
			addressing_data[addressing] = addressing_data[addressing] + addressing_count
			
		#endfor
		
		port_data = {}
		
		#port_count
		statistics_task_port_pattern = 'statistics_task_port-*'
		statistics_task_port_keys = r.keys(statistics_task_port_pattern)
		
		for statistics_task_port_key in statistics_task_port_keys:
			statistics_task_port_key = statistics_task_port_key.decode()
			
			port = statistics_task_port_key.split("-")[-1]
			port_count = int(r.get(statistics_task_port_key).decode())
			
			if port not in port_data:
				port_data[port] = 0
			#endif
			port_data[port] = port_data[port] + port_count
			
		#endfor
				
		data  = {
					'addressing': addressing_data,
					'port': port_data,
				}


	return response(200, "ok", data)

#-------------------------------------------------------------------------------

def errorlist(request):
	

	if request.method == 'GET':
		
		#--------------------
		task_total = 0
		error_jobs = []
		
		error_job_set_pattern = 'error_job-*'
		error_job_set_keys = r.keys(error_job_set_pattern)


		for error_job_set_key in error_job_set_keys:
			jobs = list(r.smembers(error_job_set_key))
			for job in jobs:
			
				job_key = job.decode()
				
				#task_total
				length = int(r.hget(job_key, 'length').decode())
				priority = int(r.hget(job_key, 'priority').decode())
				task_total = task_total + length
				
				#latest
				description = r.hget(job_key, 'description').decode()
				job_id = job_key.split("-")[-1]
				create_time = r.hget(job_key, 'create_time').decode()
				item = (create_time, length, job_id, priority, description, encrypt(job_key))
				error_jobs.append(item)
			
			#endfor
		#endfor
		
		
		#job_latest sort by create_time

		error_jobs = sorted(error_jobs, key=lambda x: (x[0]))
		
		
		#-------------------------------
		error_tasks = []
		
		
		error_task_set_pattern = 'error_task-*'
		error_task_set_keys = r.keys(error_task_set_pattern)

		for error_task_set_key in error_task_set_keys:
			tasks = list(r.smembers(error_task_set_key))
		
			for task in tasks:
				
				task_key = task.decode()
				
				item = {}
				
				tmp = task_key.split("-")
				worker_group = tmp[1]
				worker_key = tmp[2]
				worker_role = tmp[3]
				job_id = tmp[4]
				job_key = 'job-' +worker_group + '-' + worker_key + '-' + worker_role + '-' + job_id
				
				item['job_id'] = job_id
		
				item['description'] = r.hget(job_key, 'description').decode()
				
				start_time = r.hget(task_key, 'start_time')
				if start_time == None:
					#ignore deleted task
					continue
				#endif
				
				addressing = r.hget(task_key, 'addressing').decode()
				if  addressing == "binary":
					item['data'] = 'BINARY'
				else:
					item['data'] = r.hget(task_key, 'data').decode()
				#endif
				
				item['port'] = r.hget(task_key, 'port').decode()
				item['addressing'] = addressing
				item['create_time'] = int(r.hget(task_key, 'create_time').decode())
				
				item['job_access_key'] = encrypt(job_key)
				item['task_access_key'] = encrypt(task_key)
				
				error_tasks.append(item)
			#endfor
		#endfor
		
		
		#job_latest sort by create_time
		error_tasks = sorted(error_tasks, key=lambda x: (x["create_time"]))
		

		
		data  = {
					'error_jobs': error_jobs,
					'error_tasks': error_tasks,
				}
		

	return response(200, "ok", data)



