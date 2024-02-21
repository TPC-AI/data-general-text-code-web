#!/bin/bash

# If you get a linker error regarding some targets in the deps/ directory
# the fastest fix I found was to manually go and compile those directories
# e.g. "cd deps/ && make CC=/opt/cray/pe/gcc/11.2.0/bin/gcc MALLOC=libc hiredis lua fpconv linenoise hdr_histogram"

# Below I explicitly change the compilation tools to use gcc and libc malloc
# for some reason by default redis uses nvc++ and jemalloc

# I'd recommend running "make test" after building to ensure everything completed
# successfully

module load gcc/11.2.0

wget https://download.redis.io/redis-stable.tar.gz
tar -xzvf redis-stable.tar.gz
cd redis-stable
make CC=/opt/cray/pe/gcc/11.2.0/bin/gcc MALLOC=libc