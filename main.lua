rados = require 'rados'
clslua = require 'clslua'

command = arg[1]
pool = arg[2]
object = arg[3]

cluster = rados.create()
cluster:conf_read_file()
cluster:connect()
ioctx = cluster:open_ioctx(pool)

function write(object, str, offset)
	print('write', object, str, offset)	
end

function write_full(object, str)
	print('write_full', object, str)	
end

function read(object)
	print('read', object)	
end

function ls_versions(object)
	print('ls_versions', object)	
end

function remove(object)
	print('ls_versions', object)	
end

actions = {
	["write"] = function() write(object, arg[4], tonumber(arg[5])) end,
	["write_full"] = function() write(object, arg[4]) end,
	["read"] = function() read(object) end,
	["lsver"] = function() ls_versions(object) end,
	["remove"] = function() remove(object) end,
}

actions[command]()
