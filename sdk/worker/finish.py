import json;
import requests;

import config;

import traceback;

import time;
def log(text):
	now = time.strftime("%Y-%m-%d %H:%M:%S");
	text = "["+now+"] "+text;
	print(text)
	
#enddef


def create_session():

	return requests.session();
#enddef


def finish_task(session, task):
	
	
	data = {}
	
	data['worker_group'] = config.worker_group
	data['worker_key'] = config.worker_key
	data['worker_role'] = config.worker_role
	
	data['worker_id'] = config.worker_id
	
	data['task'] = task
	
	
	url = 'http://' + config.phew_server + '/task/finish'
	timeout = 30
	
	try:
		resp = session.post(url = url, data = json.dumps(data), timeout = timeout);

		ret = json.loads(resp.text)
		
		log('[finish_task]' + str(ret['code']) +  ":"  +  ret['msg'])
		if ret['code'] == 200:
			return True
		#endif
	except:
		traceback.print_exc()
		log('finish task error')
	
	#endtry
	
	return False
	
#enddef


def main():

	
	task = {}
	task['job_id'] = 4399
	task['hash'] = '4a3af7e431970ad0a3b37c1ae20f1506'
	task['state'] = 'done'
	task['note'] = ''
	
	result = {}
	result['ocr'] = 'thisistheresult'
	
	task['result'] = json.dumps(result)

	session = create_session()
	rs = finish_task(session = session, task = task)
	
	print(rs)
	
#endmain


if __name__ == '__main__':
	main()
#end
	
	
	
	