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


def get_task(session):
	
	task = None
	
	data = {}
	
	data['worker_group'] = config.worker_group
	data['worker_key'] = config.worker_key
	data['worker_role'] = config.worker_role
	
	data['worker_id'] = config.worker_id
	
	url = 'http://' + config.phew_server + '/task/get'
	timeout = 30
	
	try:
		resp = session.post(url = url, data = json.dumps(data), timeout = timeout);

		ret = json.loads(resp.text)
		
		log('[get_task]' + str(ret['code']) +  ":"  +  ret['msg'])
		if ret['code'] == 200:
			task = ret['data']
		#endif
		if ret['code'] == 404:
			task = ''
		#endif
	except:
		traceback.print_exc()
		log('get task error')
	
	#endtry
	
	return task
	
#enddef


def main():


	session = create_session()
	rs = get_task(session = session)
	
	print(rs)
	
#endmain


if __name__ == '__main__':
	main()
#end
	
	
	
	