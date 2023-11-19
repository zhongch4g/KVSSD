sync
sysctl -q -w vm.drop_caches=3
echo 3 >/proc/sys/vm/drop_caches
