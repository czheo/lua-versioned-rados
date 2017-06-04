#! /usr/bin/env lua

local argparse = require "argparse"

local parser = argparse()
parser:option("-p --pool", "pool", "data")
parser:option("-m --module", "module", "full_copy_in_obj")
parser:command_target("command")

local put_parser = parser:command("put")
put_parser:argument "key"
put_parser:argument "path"

local get_parser = parser:command("get")
get_parser:argument "key"
get_parser:argument("ver"):args '?'

local lsver_parser = parser:command("lsver")
lsver_parser:argument "key"

local args = parser:parse()
-- load module
local m = require(args['module'])

ioctx = m.connect(args['pool'])
if args['put'] then
  data = utils.read_file(args['path'])
  uid = m.put(ioctx, args['key'], data)
  print(uid)
elseif args['get'] then
  if args['ver'] then
    -- get version
    ret, data = m.getver(ioctx, args['key'], args['ver'])
    if ret then
      io.write(data)
    else
      utils.perror('Version not found: ' .. args['key'] .. '@' .. args['ver'])
    end
  else
    -- get HEAD
    ret, data = m.get(ioctx, args['key'])
    if ret then
      io.write(data)
    else
      utils.perror('Key not found: ' .. args['key'])
    end
  end
elseif args['lsver'] then
  ret, data = m.lsver(ioctx, args['key'])
  print(data)
else
end
