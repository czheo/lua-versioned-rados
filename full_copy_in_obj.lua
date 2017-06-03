#! /usr/bin/env lua

rados = require 'rados'
clslua = require 'clslua'
inspect = require 'inspect'
utils = require 'utils'

function put(ioctx, key, path)
  payload = utils.read_file(path)
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
        -- head = objclass.map_get_val('HEAD')
        -- create_commit(uid, hash, payload_bl, head)
      end
    end
    objclass.register(put)
  ]]
  return clslua.exec(ioctx, key, script, 'put', input)
end

function get(ioctx, key)
  script = [[
    function get(input, output)
      head = objclass.map_get_val('HEAD')
      data = objclass.map_get_val('commit/' .. head:str())
      output:append(data:str())
    end
    objclass.register(get)
  ]]
  return clslua.exec(ioctx, key, script, 'get', "")
end

function lsver()
end

function main(args)
  pool = args['pool']
  cluster = rados.create()
  cluster:conf_read_file()
  cluster:connect()
  ioctx = cluster:open_ioctx(pool)
  if args['put'] then
    ret, data = put(ioctx, args['key'], args['path'])
    if ret then
      print(data)
    else
      print(ret, data)
    end
  elseif args['get'] then
    ret, data = get(ioctx, args['key'])
    if ret then
      io.write(data)
    else
      utils.perror('Key not found: ' .. args['key'])
    end
  elseif args['lsver'] then
    lsver()
  else
  end
end

local argparse = require "argparse"

local parser = argparse("script", "An example.")
parser:option("-p --pool", "pool", "data")
parser:command_target("command")

local put_parser = parser:command("put")
put_parser:argument "key"
put_parser:argument "path"

local get_parser = parser:command("get")
get_parser:argument "key"

local get_parser = parser:command("lsver")

local args = parser:parse()
main(args)
