
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



