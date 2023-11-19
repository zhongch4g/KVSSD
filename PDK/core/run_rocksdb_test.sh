# cd /home/chenzhong/projects/nvmevirt
# sudo bash build_nvmevirt.sh

cd /home/chenzhong/projects/KVSSD/PDK/core

sudo rm -rf build
mkdir build
cd build
cmake -DWITH_SPDK=ON -DCMAKE_BUILD_TYPE=release ..
make -j4

ksize=16
vsize=512
nthreads=1
result_path="../results/nvmevirt_rocksdb_kvssd"
prefix="NVMEVIRT_NVM"
file_name="rocksdb_k${ksize}_v${vsize}_thrd${nthreads}_rnd_block_cache"

collect_pstat_stats() {
    dstat -c -m -d -D total,nvme1n1 -r -fs -T --output ${result_path}/${prefix}_${file_name}.csv 1 &
    PIDPSTATSTAT1=$!
}

collect_iostat_stats() {
    iostat -yxmt 1 nvme1n1 > ${result_path}/${prefix}_${file_name}.log &
    PIDPSTATSTAT2=$!
}

collect_pstat_stats
collect_iostat_stats
sudo rm -rf /mnt/nvmevirt/rocksdb_temp
sudo rm ${result_path}/${prefix}_${file_name}.*
ulimit -Sn 204800
sudo ./rocksdb_application --path=/mnt/nvmevirt/rocksdb_temp --benchmarks=load,readlatest,readlatest,readlatest,readlatest --worker_threads=${nthreads} --num=10000000 --key_size=${ksize} --value_size=${vsize} --report_interval=1 --batch=100 \
| tee ${result_path}/${prefix}_${file_name}.data


# kill stats
set +e
kill $PIDPSTATSTAT1
kill $PIDPSTATSTAT2
set -e
sudo kill -9 $(pidof iostat)
sudo kill -9 $(pidof dstat)