---------------------------------------------
## A Free-Space Adaptive Runtime Zone-Reset Algorithm for Enhanced ZNS Efficiency

This is the FAR prototype for the ACM'23 HotStorage submission.

FAR is built as an extension of ZenFS.

0. There are 3 scripts. Set RAW Paths of ZNS, LOG(MANIFEST), RocksDB, and benchmarking result that matched to your environment. libzbd is required.

1. script/compile.sh
- Compiling Rocksdb, ZenFS

2. script/init.sh
- Set NVME ZNS scheduler to mq-deadline, and make ZenFS user level file system on raw ZNS device.

3. benchmark.sh
- EZReset, LZReset, FAR will benchmarked through loop. You can modify T value on line 28.
