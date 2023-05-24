## PATH
ROCKSDB_PATH=/home/gs201/Desktop/rocksdb
RESULT_DIR_ROOT_PATH=/home/gs201/testdata/zns_timelapse
LOG_PATH=/home/gs201/log_
RAW_ZNS=nvme0n1
RAW_ZNS_PATH=/sys/block/${RAW_ZNS}/queue/scheduler


## Try until db_bench success
RETRY=0

## Algorithm
EAGER=0
LAZY=1

## Variations of FAR
FAR=2
LAZY_LOG=3
LAZY_LINEAR=4
LAZY_EXP=9

## Dataset
SMALL=9437184 # 9
MED=12582912 # 12
LARGE=15728640 # 15

## Tuning Point
T=80




while :
do
    FAILED=0
    for i in 1 2 3 4 5
    do
        for ALGORITHM in $LAZY $EAGER $LAZY_LINEAR
        do
        if [ $ALGORITHM -eq $EAGER ]; then
            RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/eager
        elif [ $ALGORITHM -eq $LAZY ]; then
            RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/lazy
        elif [ $ALGORITHM -eq $LAZY_LINEAR ]; then
            RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/${T}_linear
        elif [ $ALGORITHM -eq $NORUNTIME ]; then
            RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/noruntime
        elif [ $ALGORITHM -eq $LAZY_LOG ]; then
            RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/${T}_log
        elif [ $ALGORITHM -eq $LAZY_EXP ]; then
            T=100
            RESULT_DIR_PATH=${RESULT_DIR_ROOT_PATH}/${T}_exp
        else 
            echo "No such Algorithm"
            exit
        fi
        
        if [ ! -d ${RESULT_DIR_PATH} ] 
        then
            echo "NO ${RESULT_DIR_PATH}"
            mkdir ${RESULT_DIR_PATH}
        fi
            for SIZE in $SMALL $LARGE $MED 
                do
                    RESULT_PATH=${RESULT_DIR_PATH}/result_${SIZE}_${i}
                    while :
                    do

                        if [ -f ${RESULT_PATH} ]; then
                            echo "already $RESULT_PATH exists"
                            break
                        fi

                        sleep 5
                        ## Initialize ZenFS
                        echo "mq-deadline" | sudo tee ${RAW_ZNS_PATH}
                        sudo rm -rf ${LOG_PATH}
                        mkdir ${LOG_PATH}
                        sudo ${ROCKSDB_PATH}/plugin/zenfs/util/zenfs mkfs --force --enable_gc --zbd=/${RAW_ZNS} --aux_path=${LOG_PATH}

                        EC=$?
                        if [ $EC -eq 254 ]; then
                            echo "Failed to open device"
                            exit
                        fi
                        sleep 10
                        echo $RESULT_PATH
                        sudo ${ROCKSDB_PATH}/db_bench -num=$SIZE -benchmarks="fillrandom,stats" --fs_uri=zenfs://dev:nvme0n1 -statistics -threads=1 -value_size=1024 \
                        -file_opening_threads=4  \
                        -max_background_flushes=4 -max_background_compactions=4 -histogram \
                        -reset_scheme=$ALGORITHM -tuning_point=$T -reset_at_foreground=true > ${RESULT_DIR_PATH}/tmp
                        EC=$?
                        if grep -q "fillrandom" ${RESULT_DIR_PATH}/tmp; then
                            cat ${RESULT_DIR_PATH}/tmp > ${RESULT_PATH}
                            break
                        fi
                        FAILED=1
                        if [ $RETRY -eq 1 ]; then
                            echo "${RESULT_PATH} failed, RETRY"
                        else
                            echo "${RESULT_PATH} failed"
                            break
                        fi   
                        sleep 15

                        if [ $EC -eq 254 ]; then
                            echo "Failed to open device"
                            exit
                        fi
                    done
                done
            sleep 20
        done
        sleep 60
    done
    
    if [ $FAILED -eq 0 ]; then
        break
    fi
done