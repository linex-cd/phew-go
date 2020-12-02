import os

vendor_id = os.getenv('VENDOR_ID', '1')
vendor_name = os.getenv('VENDOR_NAME', 'Nanjing-Dev-vendor-node-1')
vendor_location = os.getenv('VENDOR_LOCATION', '南京')

phew_server = os.getenv('PHEW_SERVER', '127.0.0.1:2020')

worker_group = os.getenv('WORKER_GROUP', 'Nanjing')
worker_key = os.getenv('WORKER_KEY', 'testkey12345')
worker_role = os.getenv('WORKER_ROLE', 'textise')



