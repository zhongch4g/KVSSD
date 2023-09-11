../tools/setup.sh

mkdir build
cd build
cmake -DWITH_SPDK=ON ..
make -j4

sudo ./sample_code_sync_dev -d 0001:10:00.0 -n 1000 -o 1 -k 16 -v 4096 | tee sync_example_unvme_v19_01_v1.log