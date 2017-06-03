import boto
import boto.s3.connection
import json
import socket
import os, sys
import threading
import argparse
import string, random, time
dir_path = os.path.dirname(os.path.realpath(__file__))


def init_bucket(conn):
    name = 'test_bucket'
    existed = conn.lookup(name)
    if existed:
        print("bucket resetting")
        bucket = conn.get_bucket('test_bucket')
        for ver in bucket.list_versions():
            bucket.delete_key(ver.name, version_id = ver.version_id)
        conn.delete_bucket('test_bucket')
    bucket = conn.create_bucket(name)
    bucket.configure_versioning(True)
    return bucket

def set(bucket, key, val):
    key = bucket.new_key(key)
    key.set_contents_from_string(val)

def get(bucket, key):
    key = bucket.get_key(key)
    return key.get_contents_as_string()


def connect():
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
        return conn

def generate_random_data(ver_nu, size):
    ret = []
    for i in xrange(ver_nu):
        data = ''.join(random.choice(string.ascii_letters) for _ in xrange(size))
        ret.append(data)
    return ret

def worker(id, bucket):
    print "worker %d started" % id
    for i in xrange(ver_nu):
        for obj_idx in xrange(obj_nu):
            if interrupted:
                print "worker %d stopped" % id
                return
            data = workload[i]
            key = 'thread%d-%d' % (id, obj_idx)
            set(bucket, key, data)
            get(bucket, key)
            counters[id] += 1

def monitor(interval):
    print "monitor start"
    while any(t.is_alive() for t in threads):
        start_cnt = sum(counters)
        time.sleep(interval)
        print (sum(counters) - start_cnt) * 1.0 / interval, 'OPS/sec'
    print "monitor stop"

def main():
    try:
        conn = connect()
        bucket = init_bucket(conn)
        for i in xrange(thread_nu):
            t = threading.Thread(target=worker, args=(i, bucket))
            threads.append(t)
            t.daemon = True
            t.start()
        mon_thread = threading.Thread(target=monitor, args=(monitor_interval, ))
        mon_thread.start()
        while any(t.is_alive() for t in threads):
            time.sleep(1)
    except (KeyboardInterrupt, SystemExit):
        print 'keyboard interrupted'
        global interrupted
        interrupted = True
        mon_thread.join()
        for t in threads:
            t.join()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--thread_nu",
            default = 6,
            type = int,
            nargs='?')
    parser.add_argument("--obj_nu",
            help="objects per thread",
            default = 20,
            type = int,
            nargs='?')
    parser.add_argument("--ver_nu",
            help="versions per object",
            default = 10,
            type = int,
            nargs='?')
    parser.add_argument("--obj_size",
            help="Kbytes",
            default = 512,
            type = int,
            nargs='?')
    parser.add_argument("--monitor_interval",
            default = 2,
            type = int,
            nargs='?')
    args = parser.parse_args()
    thread_nu = args.thread_nu
    obj_nu = args.obj_nu
    ver_nu = args.ver_nu
    obj_size = args.obj_size * 1024
    monitor_interval = args.monitor_interval

    # global var
    threads = []
    counters = [0] * thread_nu
    interrupted = False
    workload = generate_random_data(ver_nu, obj_size)
    main()

