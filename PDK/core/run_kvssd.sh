# write 1GB
# 1 4KB 2*4KB 4*4KB 8*4KB 16*4KB 
# 262144 131072 65536 32768 16384
# nnsize=(4096 8192 16384 32768 65536)
# nr_ops=(262144 131072 65536 32768 16384)
nnsize=(16 32 64 128 256 512)
nr_ops=(10000000 10000000 10000000 10000000 10000000 10000000)
for idx in 0;
do
    cd /home/virtroot/linux-5.19/nvmevirt-dev
    sudo bash build_nvmevirt.sh

    cd /home/virtroot/KVSSD/PDK/core

    sudo bash tools/setup.sh
    sudo rm -rf build
    mkdir build
    cd build
    cmake -DWITH_SPDK=ON -DCMAKE_BUILD_TYPE=release ..
    make -j4

    batch_length=64
    ksize=16
    vsize=${nnsize[${idx}]}
    nthreads=1
    result_path="../results/mempool"
    prefix="NVMEVIRT_KVSSD_mempool"
    file_name="k${ksize}_v${vsize}_thrd${nthreads}_sync_batch${batch_length}"
    qdepth=64
    num=${nr_ops[${idx}]}


    # read -p "Sync[1] or Async[2]: " type
    type=1
    # [8, 32, 128, 512, 1024, 2048, 4096]
    if [ ${type} -eq 1 ]
    then
    echo "sync io"
    sudo ./sample_sync_application --device_path=0001:10:00.0 --keyspace_name=keyspace_test --benchmarks=load_batch --batch_length=${batch_length} --thread=${nthreads} --num=${num} --key_size=${ksize} --value_size=${vsize} --report_interval=1 --batch=100 \
    # | tee ${result_path}/${prefix}_${file_name}.data
    else
    echo "async io"
    sudo ./sample_async_application --device_path=0001:10:00.0 --keyspace_name=keyspace_test --benchmarks=load --thread=${nthreads} --num=${num} --key_size=${ksize} --value_size=${vsize} --report_interval=1 --batch=100 --qdepth=${qdepth} \
    | tee ${result_path}/${prefix}_${file_name}_async_qdepth${qdepth}.data
    fi
done