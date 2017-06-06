#! /usr/bin/env lua

local p = require 'posix'
local inspect = require 'inspect'
local rados = require 'rados'
local socket = require 'socket'
-- global var
pidlist = {}

-- spawn process
function spawn(fn, ...)
  local cpid = p.fork()
  if cpid == 0 then -- child reads from pipe
    fn(unpack(arg))
  else -- parent writes to pipe
    return cpid
  end
end

function worker(id) 
  print('start worker', id)
  local ioctx = m.connect('data')
  for i = 1, ver_nu do
    for j = 1, obj_nu do
      key = 'obj-' .. j
      -- key = 'thread-' .. id .. '-' .. j
      if args.op == 'set' then
        data = workloads[i]
        m.put(ioctx, key, data)
      else
        m.get(ioctx, 'obj-' .. j)
      end
    end
  end
  p._exit(0)
end

function main()
  if args.op == 'set' then
    os.execute('make purge')
  end
  start_t = socket.gettime()
  for i = 1, thread_nu do
    pid = spawn(worker, i)
    table.insert(pidlist, pid)
  end
  for i, pid in ipairs(pidlist) do
    p.wait(pid)
  end
  time = socket.gettime() - start_t
  print('total time = ', time, 'sec')
  ops = thread_nu * obj_nu * ver_nu
  print('AVG throughput = ', ops / time, 'Ops/sec')
  print('AVG throughput = ', ops / time * obj_size / 1024, 'MB/sec')
end

local charset = {}
for i = 48, 57 do table.insert(charset, string.char(i)) end
for i = 65, 90 do table.insert(charset, string.char(i)) end
for i = 97, 122 do table.insert(charset, string.char(i)) end
function random_string(len)
  ret = ''
  for i = 1, len do
    ret = ret .. charset[math.random(1, #charset)]
  end
  return ret
end

function generate_workloads(ver_nu, obj_size)
  local ret = {}
  for i = 1, ver_nu do
    str = random_string(1024):rep(obj_size)
    print('generating workload', i, 'size=', #str/1024, 'KB')
    table.insert(ret, str)
  end
  return ret
end

-- parse arguments
local argparse = require "argparse"
local parser = argparse()
parser:option("-m --module", 'module', 'full_copy_in_obj')
parser:option("--op", nil, 'set')
parser:option("--thread_nu", nil, 6,
  tonumber)
parser:option("--obj_nu", nil, 20,
  tonumber)
parser:option("--ver_nu", nil, 10,
  tonumber)
parser:option("--obj_size", nil, 512,
  tonumber)
args = parser:parse()
thread_nu = args.thread_nu
obj_nu = args.obj_nu
ver_nu = args.ver_nu
obj_size = args.obj_size
if args.module == 'raw' then
  m = {
    connect = function (pool)
      cluster = rados.create()
      cluster:conf_read_file()
      cluster:connect()
      ioctx = cluster:open_ioctx(pool)
      return ioctx
    end,
    get = function(ioctx, key)
        size, mtime = ioctx:stat(key)
        return ioctx:read(key, size, 0)
    end,
    put = function(ioctx, key, data)
        ioctx:write(key, data, #data, 0)
    end
  }
else
  m = require(args.module)
end
print('thread_nu =', thread_nu)
print('obj_nu =', obj_nu)
print('ver_nu =', ver_nu)
print('obj_size=', obj_size)
print('module =', args.module)
print('op =', args.op)

if args.op == 'set' then
  workloads = generate_workloads(ver_nu, obj_size)
end

main()
