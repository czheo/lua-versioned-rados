dependency:
	luarocks install argparse
	luarocks install https://raw.github.com/noahdesu/lua-rados/master/rockspecs/lua-rados-0.0.2-1.rockspec
	luarocks install sha2
	luarocks install inspect luaposix luasock

purge:
	rados purge data --yes-i-really-really-mean-it
	rados purge data.ver --yes-i-really-really-mean-it
