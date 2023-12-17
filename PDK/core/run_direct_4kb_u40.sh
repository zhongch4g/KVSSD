# cd /home/chenzhong/projects/nvmevirt
# sudo bash build_nvmevirt.sh

cd /home/chenzhong/projects/KVSSD/PDK/core

rm -rf build
mkdir build
cd build
cmake -DWITH_SPDK=ON -DCMAKE_BUILD_TYPE=release ..
make -j4

ksize=16
vsize=4096
nthreads=1
result_path="../results/mempool-Dec8-2023"
prefix="direct_4kb"
# file_name="direct_k${ksize}_v${vsize}_thrd${nthreads}_rnd_sync"
file_name="k${ksize}_v${vsize}_thrd${nthreads}_rnd"
# num=$((256 * 1024 * 1024 / vsize))
num=1000000

mkdir -p ${result_path}

collect_pstat_stats() {
    dstat -c -m -d -D total,nvme0n1 -r -fs -T --output ${result_path}/${prefix}_${file_name}.csv 1 &
    PIDPSTATSTAT1=$!
}

collect_iostat_stats() {
    iostat -yxmt 1 nvme0n1 > ${result_path}/${prefix}_${file_name}.log &
    PIDPSTATSTAT2=$!
}

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

# collect_pstat_stats
collect_iostat_stats

ulimit -Sn 204800
sudo ./mempool_hash_application --path=/mnt/nvmevirt/mempool_hash --benchmarks=load4k --worker_threads=${nthreads} --num=${num} --key_size=${ksize} --value_size=${vsize} --report_interval=1 --batch=100 | tee ${result_path}/${prefix}_${file_name}.data


# kill stats
# set +e
# kill $PIDPSTATSTAT1
kill $PIDPSTATSTAT2
# set -e
# sudo kill -9 $(pidof iostat)
# sudo kill -9 $(pidof dstat)
