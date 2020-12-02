# api for vendor
import redis

#r = redis.Redis(host = '192.168.2.29', port = 2019, db = 0);
r = redis.Redis(host = '127.0.0.1', port = 2019, db = 0);


####################################################################
#进行中的任务
job_key_pattern = 'job-*'

job_keys = r.keys(job_key_pattern)

for job_key in job_keys:
	
	job_key = job_key.decode()
	
	state = r.hget(job_key, 'state').decode()
	if state == 'assigned':
		r.hset(job_key, 'state', 'deleted')
		task_key_pattern = job_key.replace('job','task')+ '-*'
		task_keys = r.keys(task_key_pattern)
		
		for task_key in task_keys:

			#delete task
			r.delete(task_key)
		#endfor
		
		print("delete job:"+job_key)
		
	#endif
	
#endfor

####################################################################
#工作队列
work_key_pattern = 'work-*'

work_keys = r.keys(work_key_pattern)

for work_key in work_keys:
	
	work_key = work_key.decode()
	
	r.delete(work_key)
	print("delete work:"+work_key)
#endfor

####################################################################
#等待队列
tasks_key_pattern = 'tasks_*'

tasks_keys = r.keys(tasks_key_pattern)

for tasks_key in tasks_keys:
	
	tasks_key = tasks_key.decode()
	
	r.delete(tasks_key)
	print("delete tasks:"+tasks_key)
#endfor


####################################################################
#已完成队列
jobs_done_key_pattern = 'jobs_done-*'

jobs_done_keys = r.keys(jobs_done_key_pattern)

for jobs_done_key in jobs_done_keys:
	
	jobs_done_key = jobs_done_key.decode()
	
	r.delete(jobs_done_key)
	print("delete jobs done:"+jobs_done_key)
#endfor


#处理中的任务计数,直接归零
#job_pending
statistics_job_pending_pattern = 'statistics_job_pending-*'
statistics_job_pending_pending_keys = r.keys(statistics_job_pending_pattern)

for statistics_job_pending_pending_key in statistics_job_pending_pending_keys:
	statistics_job_pending_pending_key = statistics_job_pending_pending_key.decode()
	r.set(statistics_job_pending_pending_key, 0)
	
#endfor

















if __name__ == '__main__':
	pass
#end

