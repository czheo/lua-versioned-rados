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
./main.lua logs [pool] [object]
./main.lua getver [pool] [version_number]
./main.lua getblob [pool] [blob_hash]
./main.lua rollback [pool] [object] [version_number]
```

### Example
``` bash
# initialize pools
$ rados mkpool data
$ rados mkpool data.ver

# put object
$ echo "hello world" > hello.txt
$ ./main.lua put data hello hello.txt
509c2c8e91b64b0b5bcd4cfab6e1539deddc60204a6b301acd794c296268954c

# get object
$ ./main.lua get data hello
hello world

# update object
$ echo "hello Ceph" >> hello.txt
$ ./main.lua put data hello hello.txt
7003994bb861cb125621874e62c7030a5862e30d61d4042f31cf30e02a157d5d

# get logs
$ ./main.lua logs data hello
commit: 7003994bb861cb125621874e62c7030a5862e30d61d4042f31cf30e02a157d5d
object: hello
parent: 509c2c8e91b64b0b5bcd4cfab6e1539deddc60204a6b301acd794c296268954c
blob: 84f7ba9f982b3d3c8112b8af7d958a2156f281471b56ff1e03fdb9660434254e
timestamp: 1495122374

commit: 509c2c8e91b64b0b5bcd4cfab6e1539deddc60204a6b301acd794c296268954c
object: hello
parent:
blob: a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447
timestamp: 1495122250

# rollback
$ ./main.lua get data hello
hello world
hello Ceph

$ ./main.lua rollback data hello 509c2c8e91b64b0b5bcd4cfab6e1539deddc60204a6b301acd794c296268954c
91cec24dca65bdd1f882bcfe2e12f3eae27289b46564543aba186b38688400c2
$ ./main.lua get data hello
hello world

```
