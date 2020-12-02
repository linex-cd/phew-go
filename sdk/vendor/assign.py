import json;
import requests;

import traceback;

import config;

import time;
def log(text):
	now = time.strftime("%Y-%m-%d %H:%M:%S");
	text = "["+now+"] "+text;
	print(text)
	
#enddef


def create_session():

	return requests.session();
#enddef


def assign_job(session, job, tasks):
	
	data = {}
	
	data['worker_group'] = config.worker_group
	data['worker_key'] = config.worker_key
	data['worker_role'] = config.worker_role
	
	data['vendor_id'] = config.vendor_id
	

	data['job'] = job
	data['tasks'] = tasks
	
	url = 'http://' + config.phew_server + '/job/assign'
	timeout = 300
	
	try:
		resp = session.post(url = url, data = json.dumps(data), timeout = timeout);

		ret = json.loads(resp.text)
		
		log('[assign_job]' + str(ret['code']) +  ":"  +  ret['msg'])
		if ret['code'] == 200:
			return True
		#endif
	except:
		traceback.print_exc()
		log('assign job error')
	
	#endtry
	
	return False
	
#enddef


def main():

	job_id = 4399
	description = '众所周知，机器学习是个氪金游戏。'
	priority = 5
	
	job_info = {}
	job_info['job_id'] = str(job_id)
	job_info['description'] = description
	job_info['priority'] = priority
	job_info['meta'] = '{"from_node":1}'
	
	tasks = []
	
	task = {}
	task['job_id'] = job_id
	task['addressing'] = 'URL'
	task['meta'] = '{"table_name":"sheet"}'
	task['port'] = 'pdf'
	task['data'] = 'http://127.0.0.1:2020/1.pdf'
	
	tasks.append(task)
	
	task = {}
	task['job_id'] = job_id
	task['addressing'] = 'URL'
	task['meta'] = '{"abc":"def"}'
	task['port'] = 'test'
	task['data'] = 'http://127.0.0.1:2020/2.pdf'
	
	tasks.append(task)
	
	session = create_session()
	rs = assign_job(session = session, job = job_info, tasks = tasks)
	
	print(rs)
	
#endmain

if __name__ == '__main__':
	main()
#end

	
	
	
	