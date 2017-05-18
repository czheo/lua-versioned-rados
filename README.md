# lua-versioned-rados

### Independency
```
luarocks install https://raw.github.com/noahdesu/lua-rados/master/rockspecs/lua-rados-0.0.2-1.rockspec
luarocks install sha2
```

### Usage
```
./main.lua put [pool] [object] [file_path]
./main.lua get [pool] [object]
./main.lua rm [pool] [object]
./main.lua lsver [pool] [object]
./main.lua getver [pool] [object] [version_number]
./main.lua rollback [pool] [object] [version_number]
```
