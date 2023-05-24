ROCKSDB_PATH=/home/gs201/Desktop/rocksdb
cd ${ROCKSDB_PATH} && sudo DEBUG_LEVEL=0 ROCKSDB_PLUGINS=zenfs make -j16 db_bench install

if [ $? -ne 0 ]; then
    exit
fi
cd ${ROCKSDB_PATH}/plugin/zenfs/util && make clean && make