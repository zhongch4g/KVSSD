# input=../results/rocksdb_vary_value_size_configured/rocksdb_NVMEVIRT_SAMSUNG_970_PRO_k16_v4096_thrd1.data
input=../results/nvmevirt_rocksdb_kvssd/NVMEVIRT_NVM_rocksdb_k16_v4096_thrd1_rnd_block_cache.log
output=NVMEVIRT_NVM_rocksdb_k16_v4096_thrd1_rnd_block_cache.png

iostat-cli --data $input --disk nvme1n1 --fig-output $output plot
file $output