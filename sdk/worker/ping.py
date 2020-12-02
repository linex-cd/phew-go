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


def ping(session):
	
	result = None
	
	data = {}
	
	data['worker_group'] = config.worker_group
	data['worker_key'] = config.worker_key
	data['worker_role'] = config.worker_role
	
	data['worker_id'] = config.worker_id
	data['worker_name'] = config.worker_name
		
	data['worker_location'] = config.worker_location

	url = 'http://' + config.phew_server + '/task/ping'
	timeout = 30
	
	try:
		resp = session.post(url = url, data = json.dumps(data), timeout = timeout);

		ret = json.loads(resp.text)
		
		log('[ping]' + str(ret['code']) +  ":"  +  ret['msg'])
		if ret['code'] == 200:
			result = ret['data']
		#endif
	except:
		traceback.print_exc()
		log('ping error')
	
	#endtry
	
	return result
	
#enddef

def main():


	session = create_session()
	rs = ping(session = session)
	
	print(rs)
	
#endmain


if __name__ == '__main__':
	main()
#end
	
	
	
	