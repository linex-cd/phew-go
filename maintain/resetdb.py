# api for vendor
import redis

#r = redis.Redis(host = '192.168.2.29', port = 2019, db = 0);
r = redis.Redis(host = '127.0.0.1', port = 2019, db = 0);


####################################################################
#保存旧数据各种计数器

print("remembering count")
old_map = {}

#总处理的任务数目
#job_total
statistics_job_total_pattern = 'statistics_job_total-*'
statistics_job_total_pending_keys = r.keys(statistics_job_total_pattern)

job_total = 0
for statistics_job_total_pending_key in statistics_job_total_pending_keys:
	statistics_job_total_pending_key = statistics_job_total_pending_key.decode()
	old_map[statistics_job_total_pending_key] = int(r.get(statistics_job_total_pending_key).decode())
	print(statistics_job_total_pending_key+":"+str(old_map[statistics_job_total_pending_key]))
#endfor

#总处理的文件数目
#task_total
statistics_task_total_pattern = 'statistics_task_total-*'
statistics_task_total_pending_keys = r.keys(statistics_task_total_pattern)

task_total = 0
for statistics_task_total_pending_key in statistics_task_total_pending_keys:
	statistics_task_total_pending_key = statistics_task_total_pending_key.decode()
	old_map[statistics_task_total_pending_key] = int(r.get(statistics_task_total_pending_key).decode())
	print(statistics_task_total_pending_key+":"+str(old_map[statistics_task_total_pending_key]))
#endfor

#处理中的任务计数,直接归零
#job_pending
statistics_job_pending_pattern = 'statistics_job_pending-*'
statistics_job_pending_pending_keys = r.keys(statistics_job_pending_pattern)

job_pending = 0
for statistics_job_pending_pending_key in statistics_job_pending_pending_keys:
	statistics_job_pending_pending_key = statistics_job_pending_pending_key.decode()
	old_map[statistics_job_pending_pending_key] = 0
	print(statistics_job_pending_pending_key+":"+str(old_map[statistics_job_pending_pending_key]))
#endfor

#寻址计数
#addressing_count
statistics_task_addressing_pattern = 'statistics_task_addressing-*'
statistics_task_addressing_keys = r.keys(statistics_task_addressing_pattern)

for statistics_task_addressing_key in statistics_task_addressing_keys:
	statistics_task_addressing_key = statistics_task_addressing_key.decode()
	old_map[statistics_task_addressing_key] = int(r.get(statistics_task_addressing_key).decode())
	print(statistics_task_addressing_key+":"+str(old_map[statistics_task_addressing_key]))
	
	
#endfor

port_data = {}

#类型计数
#port_count
statistics_task_port_pattern = 'statistics_task_port-*'
statistics_task_port_keys = r.keys(statistics_task_port_pattern)

for statistics_task_port_key in statistics_task_port_keys:
	statistics_task_port_key = statistics_task_port_key.decode()
	old_map[statistics_task_port_key] = int(r.get(statistics_task_port_key).decode())
	print(statistics_task_port_key+":"+str(old_map[statistics_task_port_key]))
#endfor

#################################
#清空数据库后恢复数据
print("flushing db")
r.flushdb()
print("restoring count")
for key in old_map:
	r.set(key, old_map[key])
	print("restore:"+key+"->"+str(old_map[key]))
#endfor
print("reset ok")
if __name__ == '__main__':
	pass
#end


