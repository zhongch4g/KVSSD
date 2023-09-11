# Step 1. build PDK (kvapi library) under /KVSSD/PDK/core 
# ./tools/install_deps.sh

# mkdir build
# cd build
# cmake -DWITH_SPDK=ON ../
# make -j4

# Step 2. only for KVStack under /KVSSD/application/kvbench
cd build_kv
cmake -DCMAKE_INCLUDE_PATH=/home/chenzhong/Projects/KVSSD/PDK/core/include -DCMAKE_LIBRARY_PATH=/home/chenzhong/Projects/KVSSD/PDK/core/build/libkvapi.so ../
make kv_bench --debug


# sudo LD_LIBRARY_PATH=/home/chenzhong/Projects/KVSSD/application/kvbench 

# ./kv_bench -f bench_config.ini