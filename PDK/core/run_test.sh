# sudo dmesg -C
cd /home/virtroot/linux-5.19/nvmevirt-dev
sudo bash build_nvmevirt.sh

cd /home/virtroot/KVSSD/PDK/core

sudo bash tools/setup.sh
sudo rm -rf build
mkdir build
cd build
cmake -DWITH_SPDK=ON -DCMAKE_BUILD_TYPE=release ..
make -j4

ksize=16
vsize=4096 # 49152
nthreads=1
result_path="../results/mempool"
prefix="NVMEVIRT_KVSSD"
file_name="direct_k${ksize}_v${vsize}_thrd${nthreads}_seq"
qdepth=64
num=1000000

# read -p "Sync[1] or Async[2]: " type
type=1
# [8, 32, 128, 512, 1024, 2048, 4096]
if [ ${type} -eq 1 ]
then
echo "sync io"
sudo ./sample_sync_application --device_path=0001:10:00.0 --keyspace_name=keyspace_test --benchmarks=load --thread=${nthreads} --num=${num} --key_size=${ksize} --value_size=${vsize} --report_interval=1 --batch=100 \
| tee ${result_path}/${prefix}_${file_name}.data
else
echo "async io"
sudo ./sample_async_application --device_path=0001:10:00.0 --keyspace_name=keyspace_test --benchmarks=load --thread=${nthreads} --num=${num} --key_size=${ksize} --value_size=${vsize} --report_interval=1 --batch=100 --qdepth=${qdepth} \
# | tee ${result_path}/${prefix}_${file_name}_async_qdepth${qdepth}.data
fi

sudo dmesg > ${result_path}/${prefix}_${file_name}_async_qdepth${qdepth}.log

# sudo ./sample_code_async -d 0001:10:00.0 -n 100 -q 64 -o 1 -k 16 -v 4096

# reset the environment
# cd /home/chenzhong/Projects/KVSSD/PDK/core
# sudo bash tools/setup.sh reset

# # sudo ./sample_code_sync -d 0001:10:00.0 -n 1500000 -o 1 -k 8 -v 4096 -t 4
