local json = require "json"

local exec
exec = function(ioctx, oid, script, func, input)
  cmd = {
    script = script,
    handler = func, 
    input = input,
  }
  packed_input = json.encode(cmd)
  ret, outdata = ioctx:exec(oid, "lua", "eval_json", packed_input, #packed_input)
  return ret, outdata
end

return {
  exec = exec,
}
