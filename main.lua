#! /usr/bin/env lua

rados = require 'rados'
clslua = require 'clslua'
sha2 = require 'sha2'

-- common args
command = arg[1]
pool = arg[2]
object = arg[3]

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

-------------------------
-- data.ver pool structure
-- HEAD/<obj1>
-- HEAD/<obj2>
-- ...
-- blob/<hash1>
-- blob/<hash2>
-- blob/<hash3>
-------------------------
vpool = pool .. '.ver'
vioctx = cluster:open_ioctx(vpool)
if not vioctx then
  print(vpool .. ' does not exist! Do:')
  print('rados mkpool ' .. vpool)
  return
end

function obj_head_exists(object)
  return vioctx:stat('HEAD/' .. object)
end

function new_obj_head(object, hash)
  vioctx:write('HEAD/' .. object, '', 0, 0)
  return vioctx:setxattr('HEAD/' .. object, 'ref', hash, #hash)
end

function get_head_hash(object)
  return vioctx:getxattr('HEAD/' .. object, 'ref')
end

function get_blob_hash(commit_hash)
  return vioctx:getxattr('blob/' .. commit_hash, 'blob')
end

function get_parent_hash(commit_hash)
  return vioctx:getxattr('blob/' .. commit_hash, 'parent')
end

function update_obj_head(object, hash)
  return vioctx:setxattr('HEAD/' .. object, 'ref', hash, #hash)
end

function write_blob(data, hash)
  blob_obj = 'blob/' .. hash
  size, mtime = vioctx:stat(blob_obj)
  if not size then
    vioctx:write(blob_obj, data, #data, 0)
  end
end

function read_blob(hash)
  blob_obj = 'blob/' .. hash
  size, mtime = vioctx:stat(blob_obj)
  if not size then
    print(blob .. ' ' .. hash .. ' does not exist')
    return nil
  end
  return vioctx:read(blob_obj, size, 0)
end

function write_commit_blob(object, parent, blob_hash)
  parent_blob = get_blob_hash(parent)
  if parent_blob == blob_hash then
    return nil
  end

  msg = commit_msg(object, parent, blob_hash)
  commit_hash = sha2.sha256hex(msg)
  write_blob(msg, commit_hash)
  blob_obj = 'blob/' .. commit_hash
  vioctx:setxattr(blob_obj, 'parent', parent, #parent)
  vioctx:setxattr(blob_obj, 'blob', blob_hash, #blob_hash)
  return commit_hash
end

function write(object, str)
  script = [[
  function write_full(input, output)
    objclass.write_full(input, #input)
  end
  objclass.register(write_full)
  ]]

  clslua.exec(ioctx, object, script, 'write_full', str)
end

function commit_msg(object, parent, blob_hash)
  return 'object: ' .. object .. '\n'
  .. 'parent: ' .. parent .. '\n'
  .. 'blob: ' .. blob_hash .. '\n'
  .. 'timestamp: ' .. os.time()
end

function commit(object, data)
  write(object, data)
  size, mtime = ioctx:stat(object)
  if not size then
    print(object .. ' does not exist')
    return
  end

  hash = sha2.sha256hex(data)

  if not obj_head_exists(object) then
    write_blob(data, hash)
    commit_hash = write_commit_blob(object, '', hash)
    if commit_hash then
      new_obj_head(object, commit_hash)
    end
  else
    head = get_head_hash(object)
    write_blob(data, hash)
    commit_hash = write_commit_blob(object, head, hash)
    if commit_hash then
      update_obj_head(object, commit_hash)
    end
  end

  return commit_hash
end

function read(object)
  size, mtime = ioctx:stat(object)
  return ioctx:read(object, size, 0)
end

function get_version_data(commit_hash)
  blob_hash = get_blob_hash(commit_hash)
  return read_blob(blob_hash)
end

function ls_versions(object)
  size, mtime = vioctx:stat('HEAD/' .. object)
  if not size then
    print(object .. ' does not exist')
    return
  end

  head = get_head_hash(object)
  print('commit: ' .. head)
  print(read_blob(head))
  print()
  
  _next = get_parent_hash(head)
  while _next ~= '' do
    print('commit: ' .. _next)
    print(read_blob(_next))
    print()
    _next = get_parent_hash(_next)
  end
end

function remove(object)
  size, mtime = ioctx:stat(object)
  if not size then
    print(object .. ' does not exist')
    return
  end
  
  script = [[
  function remove(input, output)
    objclass.remove()
  end
  objclass.register(remove)
  ]]

  clslua.exec(ioctx, object, script, 'remove', '')
end

function put(object, path)
  file = io.open(path, "r")
  if not file then
    print(path .. ' not found')
    return
  end
  data = file:read "*a" -- *a or *all reads the whole file
  file:close()
  print(commit(object, data))
end

function get(object)
  size, mtime = ioctx:stat(object)
  if not size then
    print(object .. ' does not exist')
    return
  end
  print(read(object))
end

function rollback(object, ver)
  data = get_version_data(ver)
  print(commit(object, data))
end

actions = {
  ["getver"] = function() print(get_version_data(arg[3])) end,
  ["getblob"] = function() print(read_blob(arg[3])) end,
  ["logs"] = function() ls_versions(object) end,
  ["rm"] = function() remove(object) end,
  ["put"] = function() put(object, arg[4]) end,
  ["get"] = function() get(object) end,
  ["rollback"] = function() rollback(object, arg[4]) end,
}

actions[command]()
