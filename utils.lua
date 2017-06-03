local sha2 = require 'sha2'
local utils = {}

-- generate sha2 hash
function utils.sha2(data)
  return sha2.sha256hex(data)
end

-- generate uniq id
local urand = assert (io.open ('/dev/urandom', 'rb')):read(64)
local counter = 0
function utils.uniq_id()
  id = utils.sha2(urand .. tostring(counter))
  counter = counter + 1
  return id
end

-- print error and exit
function utils.perror(msg)
  io.stderr:write(msg .. '\n')
  os.exit(1)
end

-- read a file from path
function utils.read_file(path)
  local file = io.open(path, "r")
  if not file then
    utils.perror('File not found: ' .. path)
  end
  data = file:read "*a" -- *a or *all reads the whole file
  file:close()
  return data
end

return utils
