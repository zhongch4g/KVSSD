# input=../results/rocksdb_vary_value_size_configured/rocksdb_NVMEVIRT_SAMSUNG_970_PRO_k16_v4096_thrd1.data
for vsize in 16 64 256 1024 4096;
do
input=../results/mempool-Dec8-2023/NVMEVIRT_NVM_mempool_hash_k16_v${vsize}_thrd1_rnd.log
output=uhash_k16_v${vsize}.png

iostat-cli --data $input --fig-size 8,4 --disk nvme0n1 --fig-output $output plot --subplots io_transfer --title ${vsize}
file $output
done