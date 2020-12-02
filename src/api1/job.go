// api for vendor

from .init import *

def ping(request):
	code = 200
	msg = 'ok'
	data = {}

	if request.method == 'POST':
		jsondata = json.loads(request.body.decode())
		
		vendor_node_key = 'vendor-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role'] + '-' + str(jsondata['vendor_id'])
		
		vendor_node_ip = request.META['REMOTE_ADDR']
		if 'HTTP_X_FORWARDED_FOR' in request.META.keys():
			vendor_node_ip =  request.META['HTTP_X_FORWARDED_FOR']
		#endif
			
		#set to online
		r.hset(vendor_node_key, 'ping_time', int(time.time()))
		r.hset(vendor_node_key, 'ip', vendor_node_ip)
		
		r.hset(vendor_node_key, 'vendor_id', jsondata['vendor_id'])
		r.hset(vendor_node_key, 'name', jsondata['vendor_name'])
		r.hset(vendor_node_key, 'location', jsondata['vendor_location'])
		r.hset(vendor_node_key, 'state', 'online')
		
		data = 'pong'
		
	else:
		code = 403
		msg = 'method not allowed'	
	#endif
	
	return response(code, msg, data)



def assign(request):
	code = 200
	msg = 'ok'
	data = {}

	if request.method == 'POST':
		jsondata = json.loads(request.body.decode())
		
		job_info = jsondata['job']
				
		#############################
		# make job record
		job_key = 'job-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role'] + '-' + str(job_info['job_id'])
		r.hset(job_key, 'state', 'assigned')
		
		# add create timestamp
		r.hset(job_key, 'create_time', int(time.time()))
		r.hset(job_key, 'finish_time', '')
		
		r.hset(job_key, 'vendor_id', jsondata['vendor_id'])
		
		r.hset(job_key, 'worker_group', jsondata['worker_group'])
		r.hset(job_key, 'worker_key', jsondata['worker_key'])
		r.hset(job_key, 'worker_role', jsondata['worker_role'])
		
		r.hset(job_key, 'meta', job_info['meta'])
		r.hset(job_key, 'description', job_info['description'])
		r.hset(job_key, 'priority', job_info['priority'])
		
		
		tasks = jsondata['tasks']
		
		length = len(tasks)
		r.hset(job_key, 'length', length)
		
		#job and task count statistics
		#job total
		statistics_job_total_key = 'statistics_job_total-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role'] 
		r.incr(statistics_job_total_key, 1)
		
		#job pending
		statistics_job_pending_key = 'statistics_job_pending-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role'] 
		r.incr(statistics_job_pending_key, 1)
		
		#task total
		statistics_task_total_key = 'statistics_task_total-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role'] 
		r.incr(statistics_task_total_key, len(tasks))
		
		#type base
		statistics_task_addressing_key_base = 'statistics_task_addressing-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role']
		
		statistics_task_port_key_base = 'statistics_task_port-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role'] 
		
		#---------------------------------
		ignore_count = 0

		for task_info in tasks:
			
			#make task records
			if task_info['addressing'] != 'binary':
				task_info['data'] = task_info['data'].encode()
			task_info['hash'] = md5(task_info['data'])
			
			task_key = 'task-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role'] + '-' + str(job_info['job_id']) + '-' + task_info['hash']
			r.hset(task_key, 'state', 'assigned')
			r.hset(task_key, 'note', '')
			r.hset(task_key, 'result', '')
			
			# add create timestamp
			r.hset(task_key, 'create_time', int(time.time()))
			r.hset(task_key, 'start_time', '')
			r.hset(task_key, 'finish_time', '')
			r.hset(task_key, 'try_times', 0)
			
			
			r.hset(task_key, 'job_id', job_info['job_id'])
			r.hset(task_key, 'priority', job_info['priority'])
			
			r.hset(task_key, 'hash', task_info['hash'])
			
			r.hset(task_key, 'meta', task_info['meta'])
			r.hset(task_key, 'addressing', task_info['addressing'])
			r.hset(task_key, 'port', task_info['port'])
			
			
			#task addressing and port count statistics
			statistics_task_addressing_key = statistics_task_addressing_key_base + '-' + task_info['addressing']
			r.incr(statistics_task_addressing_key, 1)
			
			statistics_task_port_key = statistics_task_port_key_base + '-' + task_info['port']
			r.incr(statistics_task_port_key, 1)
			
			#skip ignore task
			if task_info['port'] == 'ignore': 
				ignore_count = ignore_count + 1
				r.hset(task_key, 'state', 'done')
				r.hset(task_key, 'note', 'ignore file')
				r.hset(task_key, 'finish_time', int(time.time()))
				r.hset(task_key, 'result', '')
				if ignore_count == length:
					#add a job to set as unread
					jobs_done_key = 'jobs_done-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role']

					r.sadd(jobs_done_key, task_info['job_id'])
					
					#add finish timestamp
					r.hset(job_key, 'finish_time', int(time.time()))
					r.hset(job_key, 'state', 'done')
					
					#job pending statistics

					r.decr(statistics_job_pending_key, 1)
					
					if int(r.get(statistics_job_pending_key).decode()) < 0:
						r.set(statistics_job_pending_key, 0)
					#endif
				#endif
				continue
			#endif
			
			
			#save binary to disk for tmp use
			if task_info['addressing'] == 'binary':
				taskdata_filename = Filedirfromhash(task_info['hash']) + task_info['hash'] + '.taskdata'
				Makedirforhash(task_info['hash'])
				writefile(taskdata_filename, task_info['data'])
				task_info['data'] = ''
			else:
				r.hset(task_key, 'data', task_info['data'])
			#endif
			
			
			
			#allocate task to work priority list
			work_key = 'work-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role'] + '-' + str(job_info['priority'])
			r.lpush(work_key, task_key)
			
			#add task to tasks_waiting to wait for job state check
			tasks_waiting_key = 'tasks_waiting-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role']+ '-' + str(job_info['job_id'])
			r.sadd(tasks_waiting_key, task_key)

		#endfor
		
		#update vendor node hit counter
		vendor_node_key = 'vendor-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role'] + '-' + str(jsondata['vendor_id'])
		
		vendor_node_hit = r.hget(vendor_node_key, 'hit')
		if vendor_node_hit == None:
			vendor_node_hit = 1
		else:
			vendor_node_hit = int(vendor_node_hit) + 1
		#endif
		
		r.hset(vendor_node_key, 'hit', vendor_node_hit)
		
		

		
	#endif
				
	return response(code, msg, data)

#enddef

def delete(request):
	code = 200
	msg = 'ok'
	data = {}

	if request.method == 'POST':
		jsondata = json.loads(request.body.decode())
		
		job_info = jsondata['job']
		
		#set state to deleted
		job_key = 'job-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role'] + '-' + str(job_info['job_id'])
		r.hset(job_key, 'state', 'deleted')
		
		#seek all task in job and delete
		task_key_pattern = 'task-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role'] + '-' + str(job_info['job_id']) + "-*"
		task_keys = r.keys(task_key_pattern)
		
		for task_key in task_keys:
			#only mark delete state
			#r.hset(task_key, 'state', 'deleted')
			
			#delete task excluding from error list
			error_task_set_key = 'error_task-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role']
			
			if not r.sismember(error_task_set_key, task_key):
				r.delete(task_key)
			#endif
			
		#endfor
	#endif	

	return response(code, msg, data)

#enddef
	
def done(request):
	code = 200
	msg = 'ok'
	data = {}

	if request.method == 'POST':
		jsondata = json.loads(request.body.decode())
		
		#seek all jobs done
		jobs_done_key = 'jobs_done-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role']
		job_keys = r.smembers(jobs_done_key)
		tmp = list(job_keys)
		
		job_keys = []
		for job_key in tmp:
			job_keys.append(job_key.decode())
		#endfor
		data['done'] = job_keys

	#endif
	
	return response(code, msg, data)
	
#enddef

def detail(request):
	code = 200
	msg = 'ok'
	data = {}

	if request.method == 'POST':
		jsondata = json.loads(request.body.decode())
		
		job_info = jsondata['job']
		
		job_key = 'job-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role'] + '-' + str(job_info['job_id'])
		
		
		data['worker_group'] = r.hget(job_key, 'worker_group').decode()
		data['worker_role'] = r.hget(job_key, 'worker_role').decode()
		

		job_info['meta'] = r.hget(job_key, 'meta').decode()
		job_info['description'] = r.hget(job_key, 'description').decode()
		job_info['priority'] = r.hget(job_key, 'priority').decode()
		job_info['length'] = r.hget(job_key, 'length').decode()
		job_info['state'] = r.hget(job_key, 'state').decode()
		job_info['create_time'] = r.hget(job_key, 'create_time').decode()
		job_info['finish_time'] = r.hget(job_key, 'finish_time').decode()
		
		data['job'] = job_info
		
		#read result from task
		tasklist = []
		
		task_key_pattern = 'task-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role'] + '-' + str(job_info['job_id']) + "-*"
		task_keys = r.keys(task_key_pattern)
		
		for task_key in task_keys:
			task_info = {}
			task_info['meta'] = r.hget(task_key, 'meta').decode()
			task_info['addressing'] = r.hget(task_key, 'addressing').decode()
			task_info['port'] = r.hget(task_key, 'port').decode()
			task_info['state'] = r.hget(task_key, 'state').decode()
			task_info['note'] = r.hget(task_key, 'note').decode()
			task_info['hash'] = r.hget(task_key, 'hash').decode()
			task_info['create_time'] = r.hget(task_key, 'create_time').decode()
			task_info['start_time'] = r.hget(task_key, 'start_time').decode()
			task_info['finish_time'] = r.hget(task_key, 'finish_time').decode()
			task_info['result'] = r.hget(task_key, 'result').decode()
			
			'''
			#read result from disk if done
			result = ''
			if task_info['state'] == 'done' or task_info['state'] == 'deleted':
				result_filename = filedirfromhash(task_info['hash']) + task_info['hash'] + '.result'
				if existfile(result_filename) == True:
					result = readfile(result_filename)
				#endif
			
			#endif
			'''

			tasklist.append(task_info)
		#endfor
		data['tasks'] = tasklist
		
	#endif
	
	return response(code, msg, data)

#enddef

def read(request):
	code = 200
	msg = 'ok'
	data = {}

	if request.method == 'POST':
		jsondata = json.loads(request.body.decode())
		
		job_info = jsondata['job']
		#mark a job as read by remove from set
		jobs_done_key = 'jobs_done-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role']

		r.srem(jobs_done_key, str(job_info['job_id']))
	
	#endif
	
	return response(code, msg, data)

#enddef


def retry(request):
	code = 200
	msg = 'ok'
	data = {}

	if request.method == 'POST':
		jsondata = json.loads(request.body.decode())
		
		job_info = jsondata['job']
	
		#read result from task
		tasklist = []
		
		task_key_pattern = 'task-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role'] + '-' + str(job_info['job_id']) + "-*"
		task_keys = r.keys(task_key_pattern)
	
		task_keys = list(task_keys)
		
		
		#repush to the right of list if timeout
		for task_key in task_keys:
			task_state = r.hget(task_key, 'state').decode()
			
			
			if task_state == 'timeout' or task_state == 'error':
				
				priority = r.hget(task_key, 'priority')
				work_key = 'work-' + jsondata['worker_group'] + '-' + jsondata['worker_key'] + '-' + jsondata['worker_role'] + '-' + str(priority)
			
				r.rpush(work_key, task_key)
				
			#endif
			
			#remove from pending set
			r.srem(tasks_pending_key, task_key)
			
		#endfor
			
	
	#endif
	
	return response(code, msg, data)

#enddef


if __name__ == '__main__':
	pass
#end



