sha2 = require 'sha2'

-- generate uniq id
urand = assert (io.open ('/dev/urandom', 'rb')):read(64)
counter = 0
function uniq_id()
  id = sha2.sha256hex(urand .. tostring(counter))
  counter = counter + 1
  return id
end
