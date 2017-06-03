import boto
import boto.s3.connection
import json
import socket
import os 
dir_path = os.path.dirname(os.path.realpath(__file__))


with open(os.path.join(dir_path, 'rgw.user')) as f:
    user_info = json.load(f)
    access_key = user_info['keys'][0]['access_key']
    secret_key = user_info['keys'][0]['secret_key']

    conn = boto.connect_s3(
        aws_access_key_id = access_key,
        aws_secret_access_key = secret_key,
        host = socket.gethostname(),
        port = 8080,
        is_secure=False,               # uncomment if you are not using ssl
        calling_format = boto.s3.connection.OrdinaryCallingFormat(),
    )
    print(conn)

    try:
        bucket = conn.get_bucket('test_bucket')
    except:
        bucket = conn.create_bucket('test_bucket')
        print 'new bucket'
    # bucket.configure_versioning(False)
    for i in range(1000):
        key = bucket.new_key('test')
        key.set_contents_from_string('hello world!')
        print key.get_contents_as_string()
    # key.set_contents_from_file('hello world!')
    # keys = bucket.list()
    # bucket.delete_keys()
    # bucket.delete()
    # for version in versions:
    #     print version.name, version.version_id
    #     print key.last_modified
