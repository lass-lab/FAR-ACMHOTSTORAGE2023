// Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
// Copyright (c) 2019-present, Western Digital Corporation
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).

#pragma once

#include <cstdint>
#if !defined(ROCKSDB_LITE) && defined(OS_LINUX)

#include <errno.h>
#include <libzbd/zbd.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <string>
#include <utility>
#include <vector>
#include <set>

#include "metrics.h"
#include "rocksdb/env.h"
#include "rocksdb/file_system.h"
#include "rocksdb/io_status.h"

namespace ROCKSDB_NAMESPACE {

class ZonedBlockDevice;
class ZonedBlockDeviceBackend;
class ZoneSnapshot;
class ZenFSSnapshotOptions;

#define ZONE_CLEANING_KICKING_POINT (20)

#define KB (1024)

#define MB (1024 * KB)

#define ZENFS_SPARE_ZONES (1)

#define ZENFS_META_ZONES (3)

#define ZENFS_IO_ZONES (40) // 20GB

#define ZONE_SIZE 512

#define DEVICE_SIZE ((ZENFS_IO_ZONES)*(ZONE_SIZE))

#define ZONE_SIZE_PER_DEVICE_SIZE (100/(ZENFS_IO_ZONES))


class ZoneList {
 private:
  void *data_;
  unsigned int zone_count_;

 public:
  ZoneList(void *data, unsigned int zone_count)
      : data_(data), zone_count_(zone_count){};
  void *GetData() { return data_; };
  unsigned int ZoneCount() { return zone_count_; };
  ~ZoneList() { free(data_); };
};

class Zone {
  ZonedBlockDevice *zbd_;
  ZonedBlockDeviceBackend *zbd_be_;
  std::atomic_bool busy_;

 public:
  explicit Zone(ZonedBlockDevice *zbd, ZonedBlockDeviceBackend *zbd_be,
                std::unique_ptr<ZoneList> &zones, unsigned int idx);

  uint64_t start_;
  uint64_t capacity_; /* remaining capacity */
  uint64_t max_capacity_;
  uint64_t wp_;
  Env::WriteLifeTimeHint lifetime_;
  std::atomic<uint64_t> used_capacity_;

  IOStatus Reset();
  IOStatus Finish();
  IOStatus Close();

  IOStatus Append(char *data, uint32_t size);
  bool IsUsed();
  bool IsFull();
  bool IsEmpty();
  uint64_t GetZoneNr();
  uint64_t GetCapacityLeft();
  bool IsBusy() { return this->busy_.load(std::memory_order_relaxed); }
  bool Acquire() {
    bool expected = false;
    return this->busy_.compare_exchange_strong(expected, true,
                                               std::memory_order_acq_rel);
  }
  bool Release() {
    bool expected = true;
    return this->busy_.compare_exchange_strong(expected, false,
                                               std::memory_order_acq_rel);
  }

  void EncodeJson(std::ostream &json_stream);

  inline IOStatus CheckRelease();
};

class ZonedBlockDeviceBackend {
 public:
  uint32_t block_sz_ = 0;
  uint64_t zone_sz_ = 0;
  uint32_t nr_zones_ = 0;

 public:
  virtual IOStatus Open(bool readonly, bool exclusive,
                        unsigned int *max_active_zones,
                        unsigned int *max_open_zones) = 0;

  virtual std::unique_ptr<ZoneList> ListZones() = 0;
  virtual IOStatus Reset(uint64_t start, bool *offline,
                         uint64_t *max_capacity) = 0;
  virtual IOStatus Finish(uint64_t start) = 0;
  virtual IOStatus Close(uint64_t start) = 0;
  virtual int Read(char *buf, int size, uint64_t pos, bool direct) = 0;
  virtual int Write(char *data, uint32_t size, uint64_t pos) = 0;
  virtual bool ZoneIsSwr(std::unique_ptr<ZoneList> &zones,
                         unsigned int idx) = 0;
  virtual bool ZoneIsOffline(std::unique_ptr<ZoneList> &zones,
                             unsigned int idx) = 0;
  virtual bool ZoneIsWritable(std::unique_ptr<ZoneList> &zones,
                              unsigned int idx) = 0;
  virtual bool ZoneIsActive(std::unique_ptr<ZoneList> &zones,
                            unsigned int idx) = 0;
  virtual bool ZoneIsOpen(std::unique_ptr<ZoneList> &zones,
                          unsigned int idx) = 0;
  virtual uint64_t ZoneStart(std::unique_ptr<ZoneList> &zones,
                             unsigned int idx) = 0;
  virtual uint64_t ZoneMaxCapacity(std::unique_ptr<ZoneList> &zones,
                                   unsigned int idx) = 0;
  virtual uint64_t ZoneWp(std::unique_ptr<ZoneList> &zones,
                          unsigned int idx) = 0;
  virtual std::string GetFilename() = 0;
  uint32_t GetBlockSize() { return block_sz_; };
  uint64_t GetZoneSize() { return zone_sz_; };
  uint32_t GetNrZones() { return nr_zones_; };
  virtual ~ZonedBlockDeviceBackend(){};
};

enum class ZbdBackendType {
  kBlockDev,
  kZoneFS,
};

class ZonedBlockDevice {
 private:
  FileSystemWrapper* zenfs_;
  std::unique_ptr<ZonedBlockDeviceBackend> zbd_be_;
  std::vector<Zone *> spare_zones;
  std::vector<Zone *> io_zones;
  std::vector<Zone *> meta_zones;
  time_t start_time_;
  std::shared_ptr<Logger> logger_;
  uint32_t finish_threshold_ = 0;

/* FAR STATS */
  std::atomic<uint64_t> bytes_written_{0};
  std::atomic<uint64_t> gc_bytes_written_{0};
  std::atomic<bool> force_zc_should_triggered_{false};
  uint64_t reset_threshold_ = 0;
  std::atomic<long> active_io_zones_;
  std::atomic<long> open_io_zones_;
  std::atomic<size_t> reset_count_{0};
  // std::atomic<int> reset_count_bg_{0};
  std::atomic<uint64_t> wasted_wp_{0};
  std::atomic<clock_t> runtime_reset_reset_latency_{0};
  std::atomic<clock_t> runtime_reset_latency_{0};

  std::atomic<uint64_t> device_free_space_;

  std::mutex compaction_refused_lock_;
  std::atomic<int> compaction_refused_by_zone_interface_{0};
  std::set<int> compaction_blocked_at_;
  std::vector<int> compaction_blocked_at_amount_;

  int zone_cleaning_io_block_ = 0;
  clock_t ZBD_mount_time_;
  bool zone_allocation_state_ = true;
  struct ZCStat{
    size_t zc_z;
    int s;
    int e;
    bool forced;
  };
  std::vector<ZCStat> zc_timelapse_;
  std::vector<uint64_t> zc_copied_timelapse_;

  std::mutex io_lock_;

  struct IOBlockStat{
    pid_t tid;
    int s;
    int e;
  };
  std::vector<IOBlockStat> io_block_timelapse_;

  // std::atomic<int> io_blocked_thread_n_{0};
  int io_blocked_thread_n_ = 0;

  enum StallCondition {kNOTSET_COND=0,kNORMAL=1,kDELAYED=2,kSTOPPED=3  };
  enum StallCause {kNOTSET_CAUSE=0,kNONE=1,kMEMTABLE_LIMIT=2,kL0FILE_COUNT_LIMIT=3,kPENDING_COMPACTION=4};
  struct WriteStallStat{
    
    StallCondition cond = StallCondition::kNOTSET_COND;
    StallCause cause = StallCause::kNOTSET_CAUSE;

    void PrintStat(void){
      std::string cond_str;
      std::string cause_str;
      switch (cond)
      {
      case StallCondition::kNORMAL:
        cond_str="NORMAL";
      break;
      case StallCondition::kDELAYED:
        cond_str="DELAYED";
        break;
      case StallCondition::kSTOPPED:
        cond_str="STOPPED";
        break;
      default:
        cond_str=" ";
        break;
      }

      switch (cause)
      {
      case StallCause::kNONE:
        cond_str=" ";
        break;
      case StallCause::kMEMTABLE_LIMIT:
        cause_str="MEMTABLE LIMIT";
        break;
      case StallCause::kL0FILE_COUNT_LIMIT:
        cause_str="L0FILE LIMIT";
        break;
      case StallCause::kPENDING_COMPACTION:
        cause_str="PENDING COMPACTION";
        break;
      default:
        cause_str=" ";
        break;
      }
      printf(" %7s | %18s |\n", cond_str.c_str(), cause_str.c_str());
    }
  };
  std::unordered_map<int,WriteStallStat> write_stall_timelapse_;
  // std::atomic<double> reset_total_time_{0.0};
  /* Protects zone_resuorces_  condition variable, used
     for notifying changes in open_io_zones_ */
  std::mutex zone_resources_mtx_;
  std::condition_variable zone_resources_;
  std::mutex zone_deferred_status_mutex_;
  IOStatus zone_deferred_status_;

  std::condition_variable migrate_resource_;
  std::mutex migrate_zone_mtx_;
  std::atomic<bool> migrating_{false};

  unsigned int max_nr_active_io_zones_;
  unsigned int max_nr_open_io_zones_;

  std::shared_ptr<ZenFSMetrics> metrics_;
  uint64_t cur_free_percent_ = 100;
  void EncodeJsonZone(std::ostream &json_stream,
                      const std::vector<Zone *> zones);
  void CalculateResetThreshold();
  uint32_t reset_scheme_;
  bool reset_at_foreground_;
  uint64_t tuning_point_;
  enum {
    kEager = 0,
    kLazy = 1,
    kFAR = 2,
    kLazy_Log = 3,
    kLazy_Linear = 4,
    kCustom = 5,
    kLogLinear = 6,
    kNoRuntimeReset = 7,
    kNoRuntimeLinear = 8,
    kLazyExponential = 9
  };

  struct FARStat{

    uint64_t free_percent_;

    size_t RC_;
    int T_;
    uint64_t R_wp_; // (%)
    uint64_t RT_;
    FARStat(uint64_t fr, size_t rc, uint64_t wwp, int T, uint64_t rt)
        : free_percent_(fr), RC_(rc), T_(T), RT_(rt) {
      if(RC_==0){
        R_wp_= 100;
      }else
        R_wp_= (ZONE_SIZE*100-wwp*100/RC_)/ZONE_SIZE;
    }
    void PrintStat(void){
      printf("[%3d] | %3ld  | %3ld |  %3ld | [%3ld] |", T_, free_percent_, RC_,
             R_wp_, (RT_ >> 20));
    }
  };

  std::vector<FARStat> far_stats_;
 public:
 
  explicit ZonedBlockDevice(std::string path, ZbdBackendType backend,
                            std::shared_ptr<Logger> logger,
                            std::shared_ptr<ZenFSMetrics> metrics =
                                std::make_shared<NoZenFSMetrics>());
  virtual ~ZonedBlockDevice();
  void SetFSptr(FileSystemWrapper* fs) { zenfs_=fs; }
  IOStatus Open(bool readonly, bool exclusive);

  Zone *GetIOZone(uint64_t offset);

  IOStatus AllocateIOZone(Env::WriteLifeTimeHint file_lifetime, IOType io_type,
                          Zone **out_zone);
  void SetZoneAllocationFailed() { zone_allocation_state_=false; }
  bool IsZoneAllocationFailed(){ return zone_allocation_state_==false; }
  IOStatus AllocateMetaZone(Zone **out_meta_zone);

  uint64_t GetFreeSpace();
  uint64_t GetUsedSpace();
  uint64_t GetReclaimableSpace();
  uint64_t GetFreePercent();
  std::string GetFilename();
  uint32_t GetBlockSize();

  inline IOStatus RuntimeReset(void){
    if(reset_scheme_==kNoRuntimeReset){
      return IOStatus::OK();
    }
    if(reset_scheme_==kNoRuntimeLinear){
      if(cur_free_percent_>tuning_point_){
        return IOStatus::OK();
      }
    }
    IOStatus s;
    clock_t start=clock();
    s= ResetUnusedIOZones();
    clock_t end=clock();
    runtime_reset_latency_.fetch_add(end-start);
    return s;
  }
  // void AddZCIOBlockedTime(clock_t t){ zone_cleaning_io_block_.fetch_add(t); }
  void AddIOBlockedTimeLapse(int s,int e) {
    std::lock_guard<std::mutex> lg_(io_lock_);
    io_block_timelapse_.push_back({gettid(),s,e});
    zone_cleaning_io_block_+=(e-s);
  }

  clock_t IOBlockedStartCheckPoint(void){
    std::lock_guard<std::mutex> lg_(io_lock_);
    clock_t ret=clock();
    io_blocked_thread_n_++;
    return ret;
  }
  void IOBlockedEndCheckPoint(int start){
    int end=clock();
    std::lock_guard<std::mutex> lg_(io_lock_);
    io_blocked_thread_n_--;
    io_block_timelapse_.push_back({gettid(),start,-1});
    if(io_blocked_thread_n_==0){
      zone_cleaning_io_block_+=(end-start);
    }
    return;
  }

  void AddZCTimeLapse(int s,int e,size_t zc_z,bool forced){
    
    if(forced==true){
      force_zc_should_triggered_.store(false);
    }
    zc_timelapse_.push_back({zc_z,s,e,forced});
  }
  void AddTimeLapse(int T);

  
  IOStatus ResetUnusedIOZones();
  // IOStatus ResetUnusedIOZonesAtBackground();
  void LogZoneStats();
  void LogZoneUsage();
  void LogGarbageInfo();

  uint64_t GetZoneSize();
  uint32_t GetNrZones();
  std::vector<Zone *> GetMetaZones() { return meta_zones; }

  void SetFinishTreshold(uint32_t threshold) { finish_threshold_ = threshold; }

  void PutOpenIOZoneToken();
  void PutActiveIOZoneToken();

  void EncodeJson(std::ostream &json_stream);

  void SetZoneDeferredStatus(IOStatus status);

  std::shared_ptr<ZenFSMetrics> GetMetrics() { return metrics_; }

  void GetZoneSnapshot(std::vector<ZoneSnapshot> &snapshot);

  int Read(char *buf, uint64_t offset, int n, bool direct);

  IOStatus ReleaseMigrateZone(Zone *zone);

  IOStatus TakeMigrateZone(Zone **out_zone, Env::WriteLifeTimeHint lifetime,
                           uint32_t min_capacity);

  void AddBytesWritten(uint64_t written) { bytes_written_.fetch_add(written); };
  void AddGCBytesWritten(uint64_t written) {
    gc_bytes_written_.fetch_add(written); 
    zc_copied_timelapse_.push_back(written);
  };

  uint64_t GetGCBytesWritten(void) { return gc_bytes_written_.load(); }
  uint64_t GetUserBytesWritten() {
    return bytes_written_.load() - gc_bytes_written_.load();
  };
  uint64_t GetTotalBytesWritten() { return bytes_written_.load(); };
  int GetResetCount() { return reset_count_.load(); }
  // int GetResetCountBG() {return reset_count_bg_.load();}
  uint64_t GetWWP() { return wasted_wp_.load();}
  


  void SetCurFreepercent(uint64_t free_percent) { cur_free_percent_=free_percent; }
  
  uint64_t CalculateFreePercent(void) {
    uint64_t device_size=(uint64_t)ZENFS_IO_ZONES*(uint64_t)ZONE_SIZE ;
    uint64_t d_free_space=device_size ;
    uint64_t writed = 0;
    for(const auto z : io_zones){
      if(z->IsBusy()){
        d_free_space-=(uint64_t)ZONE_SIZE;
      }else{
        // uint64_t wp_mb=(z->wp)>>20
        // d_free_space -=(uint64_t)( z->wp_- z->start_ ) >>20 ;
        writed+=z->wp_-z->start_;
      }
    }
    
    // printf("df1 %ld\n",d_free_space);
    d_free_space-=(writed>>20);
    // printf("df 2%ld\n",d_free_space);
    device_free_space_.store(d_free_space);
    cur_free_percent_= (d_free_space*100)/device_size;
    CalculateResetThreshold();
    return cur_free_percent_;
  }

  void WriteStallCheckPoint(int T,int write_stall_cause,int write_stall_cond){
    write_stall_cause++;
    write_stall_cond++;
    StallCause cause=(StallCause) write_stall_cause;
    StallCondition cond = (StallCondition) write_stall_cond;
    if(write_stall_timelapse_[T].cond<cond ){
      write_stall_timelapse_[T].cond=cond;
      write_stall_timelapse_[T].cause=cause;
    }
  }

  bool PreserveZoneSpace(uint64_t approx_size) {
    ResetUnusedIOZones();
    approx_size>>=20;
    std::lock_guard<std::mutex> lg_(compaction_refused_lock_);

    uint64_t tmp = device_free_space_.load();
    // printf("@@@ preserve free space : approx size : %ld, left %ld\n",approx_size,tmp);
    if(tmp<approx_size){
      // compaction_refused_by_zone_interface_.fetch_add(1);
      int s=zenfs_->GetMountTime();
    
      // compaction_refused_lock_
      compaction_blocked_at_amount_.push_back(s);
      compaction_blocked_at_.emplace(s);
      force_zc_should_triggered_.store(true);
      return false;
    }
    device_free_space_.store(tmp-approx_size);
    return true;
  }
  bool ShouldForceZCTriggered(void) { return force_zc_should_triggered_.load(); }
  
  IOStatus ResetAllZonesForForcedNewFileSystem(void);
  void SetResetScheme(uint32_t r,bool f,uint64_t T) { 
    reset_scheme_=r; 
    reset_at_foreground_=f;
    tuning_point_=T;
  }
  bool ResetAtForeground() { return reset_at_foreground_;}
  bool ResetAtBackground() { return !reset_at_foreground_;}
 private:
  IOStatus GetZoneDeferredStatus();
  bool GetActiveIOZoneTokenIfAvailable();
  void WaitForOpenIOZoneToken(bool prioritized);
  IOStatus ApplyFinishThreshold();
  IOStatus FinishCheapestIOZone();
  IOStatus GetBestOpenZoneMatch(Env::WriteLifeTimeHint file_lifetime,
                                unsigned int *best_diff_out, Zone **zone_out,
                                uint32_t min_capacity = 0);
  IOStatus GetAnyLargestRemainingZone(Zone** zone_out,uint32_t min_capacity = 0);
  IOStatus AllocateEmptyZone(Zone **zone_out);

  inline uint64_t LazyLog(uint64_t sz,uint64_t fr,uint64_t T);

  inline uint64_t LazyLinear(uint64_t sz,uint64_t fr,uint64_t T);
  inline uint64_t Custom(uint64_t sz,uint64_t fr,uint64_t T);
  inline uint64_t LogLinear(uint64_t sz,uint64_t fr,uint64_t T);
  inline uint64_t LazyExponential(uint64_t sz, uint64_t fr, uint64_t T);
};

}  // namespace ROCKSDB_NAMESPACE

#endif  // !defined(ROCKSDB_LITE) && defined(OS_LINUX)
