#! /usr/bin/env lua

rados = require 'rados'
clslua = require 'clslua'
sha2 = require 'sha2'

-- common args
command = arg[1]
pool = arg[2]
object = arg[3]
vpool = pool .. '.ver'

-- connect to cluster
cluster = rados.create()
cluster:conf_read_file()
cluster:connect()

-- open pools
-- data pool
ioctx = cluster:open_ioctx(pool)
if not ioctx then
  print(pool .. ' does not exist! Do:')
  print('rados mkpool ' .. pool)
  return
end

-- data.ver pool
vioctx = cluster:open_ioctx(vpool)
if not vioctx then
  print(vpool .. ' does not exist! Do:')
  print('rados mkpool ' .. vpool)
  return
end


function write(object, str, offset)
  ioctx:write(object, str, #str, offset)
end

function commit(object)
  size, mtime = ioctx:stat(object)
  if not size then
    print(object .. ' does not exist')
    return
  end

  head = ioctx:getxattr(object, 'HEAD')
  data = ioctx:read(object, size, 0)
  hash = sha2.sha256hex(data)

  if hash == head then
    -- file not changed since previous commit
    print('nothing to commit')
  else
    -- write data to data.ver pool
    vobj = object .. '.' .. hash
    size, mtime = vioctx:stat(vobj)
    if not size then
      vioctx:write(vobj, data, #data, 0)
      if head then
        vioctx:setxattr(vobj, 'PREV', head, #head)
      end
    end

    -- update prev pointer
    ioctx:setxattr(object, 'HEAD', hash, #hash)
  end

  return hash
end

function read(object)
  size, mtime = ioctx:stat(object)
  return ioctx:read(object, size, 0)
end

function read_version(object, version)
  vobj = object .. '.' .. version
  size, mtime = vioctx:stat(vobj)
  print(vioctx:read(vobj, size, 0))
end

function ls_versions(object)
  size, mtime = ioctx:stat(object)
  if not size then
    print(object .. ' does not exist')
    return
  end

  head = ioctx:getxattr(object, 'HEAD')
  print(head)

  vobj = object .. '.' .. head
  prev = vioctx:getxattr(vobj, 'PREV')
  while prev do
    print(prev)
    vobj = object .. '.' .. prev 
    prev = vioctx:getxattr(vobj, 'PREV')
  end
end

function remove(object)
  size, mtime = ioctx:stat(object)
  if not size then
    print(object .. ' does not exist')
    return
  end

  -- set delete flag
  ioctx:setxattr(object, 'DELETE', 1, 1)
end

function put(object, path)
  file = io.open(path, "r")
  if not file then
    print(path .. ' not found')
    return
  end
  data = file:read "*a" -- *a or *all reads the whole file
  file:close()
  write(object, data, 0)
  print(commit(object))
end

function get(object)
  size, mtime = ioctx:stat(object)
  if not size then
    print(object .. ' does not exist')
    return
  end
  del_flag = ioctx:getxattr(object, 'DELETE')
  if del_flag then
    print(object .. ' is deleted')
    return
  end
  print(read(object))
end

function rollback(object, ver)
  print('rollback to a specific version')
end

actions = {
  ["getver"] = function() read_version(object, arg[4]) end,
  ["lsver"] = function() ls_versions(object) end,
  ["rm"] = function() remove(object) end,
  ["put"] = function() put(object, arg[4]) end,
  ["get"] = function() get(object) end,
  ["rollback"] = function() get(object) end,
}

actions[command]()
