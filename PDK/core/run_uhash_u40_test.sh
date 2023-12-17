# cd /home/chenzhong/projects/nvmevirt
# sudo bash build_nvmevirt.sh

nnsize=(16 32 64 128 256 512)
nr_ops=(10000000 10000000 10000000 10000000 10000000 10000000)
for idx in 0;
do

cd /home/virtroot/KVSSD/PDK/core

rm -rf build
mkdir build
cd build
cmake -DWITH_SPDK=ON -DCMAKE_BUILD_TYPE=release ..
make -j4

ksize=16
vsize=${nnsize[${idx}]}
nthreads=1
result_path="../results/mempool-Dec8-2023"
prefix="NVMEVIRT_NVM"
# file_name="direct_k${ksize}_v${vsize}_thrd${nthreads}_rnd_sync"
file_name="mempool_hash_k${ksize}_v${vsize}_thrd${nthreads}_rnd_slru"
num=${nr_ops[${idx}]}

collect_pstat_stats() {
    dstat -c -m -d -D total,nvme1n1 -r -fs -T --output ${result_path}/${prefix}_${file_name}.csv 1 &
    PIDPSTATSTAT1=$!
}

collect_iostat_stats() {
    iostat -yxmt 1 nvme1n1 > ${result_path}/${prefix}_${file_name}.log &
    PIDPSTATSTAT2=$!
}

# collect_pstat_stats
# collect_iostat_stats
sudo rm -rf /mnt/nvmevirt/mempool_hash
sudo rm ${result_path}/${prefix}_${file_name}.*
sudo touch /mnt/nvmevirt/mempool_hash

sudo rm /mnt/nvmevirt/ssd_test
sudo touch /mnt/nvmevirt/ssd_test

sudo rm ./ssd_file
sudo touch ./ssd_file

# clear the cache
sync
sysctl -q -w vm.drop_caches=3
echo 3 >/proc/sys/vm/drop_caches
sleep 5

ulimit -Sn 204800
sudo ./mempool_hash_application --path=/mnt/nvmevirt/mempool_hash --benchmarks=load,readall --worker_threads=${nthreads} --num=${num} --key_size=${ksize} --value_size=${vsize} --report_interval=1 --batch=100 | tee ${result_path}/${prefix}_${file_name}.data


# kill stats
# set +e
# kill $PIDPSTATSTAT1
# kill $PIDPSTATSTAT2
# set -e
# sudo kill -9 $(pidof iostat)
# sudo kill -9 $(pidof dstat)

done