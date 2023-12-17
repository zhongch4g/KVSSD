# sudo dmesg -C
nnsize=(16 64 256 1024 4096)
nr_ops=(5000000 5000000 5000000 5000000 5000000)
for idx in 4;
do
cd /home/chenzhong/projects/linux-5.19/nvmevirt-dev
sudo bash build_nvmevirt.sh

cd /home/chenzhong/projects/KVSSD/PDK/core

sudo bash tools/setup.sh
sudo rm -rf build
mkdir build
cd build
cmake -DWITH_SPDK=ON -DCMAKE_BUILD_TYPE=release ..
make -j4

ksize=16
vsize=${nnsize[${idx}]} # 49152
nthreads=1
result_path="../results/mempool-Dec8-2023"
prefix="dhash"
file_name="k${ksize}_v${vsize}_delay"
qdepth=64
num=$((256 * 1024 * 1024 / vsize))

# clear the cache
sync
sysctl -q -w vm.drop_caches=3
echo 3 >/proc/sys/vm/drop_caches
sleep 5

# read -p "Sync[1] or Async[2]: " type
type=1
# [8, 32, 128, 512, 1024, 2048, 4096]
if [ ${type} -eq 1 ]
then
echo "sync io"
sudo ./sample_sync_application --device_path=0001:10:00.0 --keyspace_name=keyspace_test --benchmarks=load --thread=${nthreads} --num=${num} --key_size=${ksize} --value_size=${vsize} --report_interval=1 --batch=100 \
# | tee ${result_path}/${prefix}_${file_name}.data
else
echo "async io"
sudo ./sample_async_application --device_path=0001:10:00.0 --keyspace_name=keyspace_test --benchmarks=load --thread=${nthreads} --num=${num} --key_size=${ksize} --value_size=${vsize} --report_interval=1 --batch=100 --qdepth=${qdepth} \
# | tee ${result_path}/${prefix}_${file_name}_async_qdepth${qdepth}.data
fi
done