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
      data = workloads[i]
      m.put(ioctx, 'thread-' .. j, data)
      -- m.put(ioctx, 'thread-' .. id .. '-' .. j, data)
      -- m.get(ioctx, 'thread-' .. id .. '-' .. j)
    end
  end
  p._exit(0)
end

function main()
  os.execute('make purge')
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
parser:option("--thread_nu", nil, 6,
  tonumber)
parser:option("--obj_nu", nil, 20,
  tonumber)
parser:option("--ver_nu", nil, 10,
  tonumber)
parser:option("--obj_size", nil, 512,
  tonumber)
local args = parser:parse()
thread_nu = args.thread_nu
obj_nu = args.obj_nu
ver_nu = args.ver_nu
obj_size = args.obj_size
workloads = generate_workloads(ver_nu, obj_size)
m = require(args.module)

main()
