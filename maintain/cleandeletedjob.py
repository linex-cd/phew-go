# api for vendor
import redis

#r = redis.Redis(host = '192.168.2.29', port = 2019, db = 0);
r = redis.Redis(host = '127.0.0.1', port = 2019, db = 0);


job_key_pattern = 'job-*'

job_keys = r.keys(job_key_pattern)

for job_key in job_keys:
	
	job_key = job_key.decode()
	
	state = r.hget(job_key, 'state').decode()
	if state == 'done':
		r.hset(job_key, 'state', 'deleted')
		task_key_pattern = job_key.replace('job','task')+ '-*'
		task_keys = r.keys(task_key_pattern)
		
		for task_key in task_keys:
			#only mark delete state
			#r.hset(task_key, 'state', 'deleted')
			
			#delete task
			r.delete(task_key)
		#endfor
		
		print(job_key)
		print(state)
	#endif
	
#endfor


if __name__ == '__main__':
	pass
#end

