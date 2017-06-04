ns = {}
local rados = require 'rados'
local clslua = require 'clslua'
local inspect = require 'inspect'
local utils = require 'utils'

function ns.connect(pool)
  cluster = rados.create()
  cluster:conf_read_file()
  cluster:connect()
  ioctx = cluster:open_ioctx(pool)
  return ioctx
end

function ns.put(ioctx, key, payload)
  uid = utils.uniq_id()
  hash = utils.sha2(payload)
  input = uid .. '@' .. hash .. '@' .. payload
  script = [[
    function read_input(input)
      input = input:str()
      sep1 = input:find('@')
      uid = input:sub(1, sep1 - 1)
      sep2 = input:find('@', sep1 +1)
      hash = input:sub(sep1+1, sep2 - 1)
      payload = input:sub(sep2+1)
      return uid, hash, payload
    end
    function write_data_blob(hash, payload_bl)
        objclass.map_set_val('blob/' .. hash, payload_bl, #payload_bl)
    end
    function create_commit(uid, hash, payload_bl, prev)
        write_data_blob(hash, payload_bl)
        uid_bl = bufferlist.new()
        uid_bl:append(uid)
        hash_bl = bufferlist.new()
        hash_bl:append(hash)
        objclass.map_set_val('commit/' .. uid, hash_bl, #hash_bl)
        if not prev then
          prev = bufferlist.new()
          prev:append('NULL')
        end
        objclass.map_set_val('commit/' .. uid .. '/prev', prev, #prev)
        objclass.map_set_val('HEAD', uid_bl, #uid_bl)
    end

    function put(input, output)
      uid, hash, payload = read_input(input)
      payload_bl = bufferlist.new()
      payload_bl:append(payload)
      ok, ret_or_val = pcall(objclass.stat)
      if not ok then
        -- new object
        objclass.create()
        create_commit(uid, hash, payload_bl, nil)
      else
        head = objclass.map_get_val('HEAD')
        create_commit(uid, hash, payload_bl, head)
      end
    end
    objclass.register(put)
  ]]
  ret, data = clslua.exec(ioctx, key, script, 'put', input)
  if not ret then
    utils.perror('Fail to put key: ' .. key)
  end
  return uid
end

function ns.get(ioctx, key)
  script = [[
    function get(input, output)
      head = objclass.map_get_val('HEAD')
      hash = objclass.map_get_val('commit/' .. head:str())
      data = objclass.map_get_val('blob/' .. hash:str())
      output:append(data:str())
    end
    objclass.register(get)
  ]]
  return clslua.exec(ioctx, key, script, 'get', "")
end

function ns.lsver(ioctx, key)
  script = [[
    function lsver(input, output)
      head = objclass.map_get_val('HEAD')
      curr = head:str()
      while curr ~= 'NULL' do
        output:append(curr .. '\n')
        curr = objclass.map_get_val('commit/' .. curr .. '/prev'):str()
      end
    end
    objclass.register(lsver)
  ]]
  return clslua.exec(ioctx, key, script, 'lsver', "")
end

function ns.getver(ioctx, key, ver)
  script = [[
    function getver(input, output)
        hash = objclass.map_get_val('commit/' .. input:str())
        data = objclass.map_get_val('blob/' .. hash:str())
        output:append(data:str())
    end
    objclass.register(getver)
  ]]
  return clslua.exec(ioctx, key, script, 'getver', ver)
end

return ns
