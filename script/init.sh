ZNS_SCHEDULER=/sys/block/nvme0n1/queue/scheduler
ROCKSDB_PATH=/home/gs201/Desktop/rocksdb
LOG_PATH=/home/gs201/log_
RAW_ZNS=/nvme0n1

echo "mq-deadline" | sudo tee ${ZNS_SCHEDULER}
sudo rm -rf ${LOG_PATH}
mkdir ${LOG_PATH}
sudo ${ROCKSDB_PATH}/plugin/zenfs/util/zenfs mkfs --force --enable_gc --zbd=${RAW_ZNS} --aux_path=${LOG_PATH}