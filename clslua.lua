local json = require "json"

local exec
exec = function(ioctx, oid, script, func, input)
  cmd = {
    script = script,
    handler = func, 
    input = input,
  }
  packed_input = json.encode(cmd)
  return ioctx:exec(oid, "lua", "eval_json", packed_input, #packed_input)
end

return {
  exec = exec,
}
