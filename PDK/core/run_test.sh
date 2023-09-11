cd /home/chenzhong/Projects/nvmevirt
sudo bash build_nvmevirt.sh

cd /home/chenzhong/Projects/KVSSD/PDK/core
sudo bash tools/setup.sh
cd build
cmake -DWITH_SPDK=ON -DCMAKE_BUILD_TYPE=release ..
make -j4

read -p "Enter a number: " number
# # [8, 32, 128, 512, 1024, 2048, 4096]
# sudo ./sample_application --device_path=0001:10:00.0 --keyspace_name=keyspace_test --benchmarks=load --thread=1 --num=1000000 --key_size=8 --value_size=${number} --report_interval=1 --batch=100 \
# | tee ../results/profiling/nvmevirt_kvssd_spdk_write_k8_v${number}_non_device_lat.data

# sudo ./sample_code_sync -d 0001:10:00.0 -n 6000000 -o 1 -k 8 -v 8 -t 2