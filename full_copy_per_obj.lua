#! /usr/bin/env lua

local ns = {}

rados = require 'rados'
clslua = require 'clslua'
utils = require 'utils'

function ns.connect(pool)
  cluster = rados.create()
  cluster:conf_read_file()
  cluster:connect()

  ioctx = cluster:open_ioctx(pool)
  vioctx = cluster:open_ioctx(pool .. '.ver')
  return {ioctx, vioctx}
end

function read_full(ioctx, key)
  size, mtime = ioctx:stat(key)
  if not size then
    utils.perror('Key not found: ' .. key)
  end
  return ioctx:read(key, size, 0)
end

function ns.get(iopair, key)
  ioctx, vioctx = unpack(iopair)
  uid = read_full(ioctx, key)
  hash = read_full(vioctx, 'commit/' .. uid)
  data = read_full(vioctx, 'blob/' .. hash)
  return 0, data
end

function write_data_blob(vioctx, hash, data)
  vioctx:write('blob/' .. hash, data, #data, 0)
end

function create_commit(vioctx, uid, hash, data, prev)
  write_data_blob(vioctx, hash, data)
  vioctx:write('commit/' .. uid, hash, #hash, 0)
  if not prev then
    prev = 'NULL'
  end
  vioctx:setxattr('commit/' .. uid, 'prev', prev, #prev)
end

function ns.lsver(iopair, key, ver)
  ioctx, vioctx = unpack(iopair)
  ret = ""
  head = read_full(ioctx, key)
  ret = ret .. head
  curr = vioctx:getxattr('commit/' .. head, 'prev')
  while curr ~= 'NULL' do
    ret = ret .. '\n' .. curr
    curr = vioctx:getxattr('commit/' .. curr, 'prev')
  end
  return 0, ret
end

-- function lock(iopair, key, uid)
--   ioctx, vioctx = unpack(iopair)
--   vioctx:write('LOCK', '', 0, 0)
--   while true do
--     locked = vioctx:getxattr('LOCK', key)
--     if locked == nil or locked == '' then
--       vioctx:setxattr('LOCK', key, uid, #uid)
--       locked = vioctx:getxattr('LOCK', key)
--       if uid == locked then
--         return 
--       end
--     end
--   end
-- end
--
-- function unlock(iopair, key)
--   vioctx:setxattr('LOCK', key, "", 0)
-- end

function ns.put(iopair, key, data)
  ioctx, vioctx = unpack(iopair)
  size, mtime = ioctx:stat(key)
  uid = utils.uniq_id()
  hash = utils.sha2(data)
  -- lock(iopair, key, uid)
  if not size then
    -- new obj
    create_commit(vioctx, uid, hash, data, nil)
    ioctx:write(key, uid, #uid, 0)
  else
    head = ioctx:read(key, size, 0)
    create_commit(vioctx, uid, hash, data, head)
    ioctx:write(key, uid, #uid, 0)
  end
  -- unlock(iopair, key)
  return uid
end

function ns.getver(iopair, key, ver)
  ioctx, vioctx = unpack(iopair)
  hash = read_full(vioctx, 'commit/' .. ver)
  data = read_full(vioctx, 'blob/' .. hash)
  return 0, data
end

return ns
