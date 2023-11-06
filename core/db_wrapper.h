//
//  db_wrapper.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//

#ifndef YCSB_C_DB_WRAPPER_H_
#define YCSB_C_DB_WRAPPER_H_

#include <string>
#include <vector>

#include "db.h"
#include "measurements.h"
#include "timer.h"
#include "utils.h"

namespace ycsbc {

class DBWrapper : public DB {
 public:
  DBWrapper(DB *db, Measurements *measurements, utils::Properties *props) : db_(db), measurements_(measurements) {
    ops_to_skip_ = std::stoi(props->GetProperty("ops_to_skip", "0"));
  }
  ~DBWrapper() {
    delete db_;
  }
  void Init() {
    db_->Init();
  }
  void Cleanup() {
    db_->Cleanup();
  }
  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result) {
    if (ops_to_skip_!= 0 && ops_performed_ == 0){
      timer.Start();
    }
    timer_.Start();
    Status s = db_->Read(table, key, fields, result);
    uint64_t elapsed = timer_.End();
    if(ops_performed_++ < ops_to_skip_) 
    {
      // no measurements
      if (ops_performed_ == ops_to_skip_){
        skipTime = timer.End();
      }
      return s;
    }
    if (s == kNotFound || s == kOK) {
      measurements_->Report(READ, elapsed);
    } else {
      measurements_->Report(READ_FAILED, elapsed);
    }
    return s;
  }
  Status Scan(const std::string &table, const std::string &key, int record_count,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result) {
    if (ops_to_skip_!= 0 && ops_performed_ == 0){
      timer.Start();
    }
    timer_.Start();
    Status s = db_->Scan(table, key, record_count, fields, result);
    uint64_t elapsed = timer_.End();
    if (ops_performed_++ < ops_to_skip_)
    {
      // no measurements
      if (ops_performed_ == ops_to_skip_){
        skipTime = timer.End();
      }
      return s;
    }
    if (s == kOK) {
      measurements_->Report(SCAN, elapsed);
    } else {
      measurements_->Report(SCAN_FAILED, elapsed);
    }
    return s;
  }
  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
    if (ops_to_skip_!= 0 && ops_performed_ == 0){
      timer.Start();
    }
    timer_.Start();
    Status s = db_->Update(table, key, values);
    uint64_t elapsed = timer_.End();
    if (ops_performed_++ < ops_to_skip_)
    {
      // no measurements
      if (ops_performed_ == ops_to_skip_){
        skipTime = timer.End();
      }
      return s;
    }
    if (s == kOK) {
      measurements_->Report(UPDATE, elapsed);
    } else {
      measurements_->Report(UPDATE_FAILED, elapsed);
    }
    return s;
  }
  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values) {
    if (ops_to_skip_!= 0 && ops_performed_ == 0){
      timer.Start();
    }
    timer_.Start();
    Status s = db_->Insert(table, key, values);
    uint64_t elapsed = timer_.End();
    if (ops_performed_++ < ops_to_skip_)
    {
      // no measurements
      if (ops_performed_ == ops_to_skip_){
        skipTime = timer.End();
      }
      return s;
    }
    if (s == kOK) {
      measurements_->Report(INSERT, elapsed);
    } else {
      measurements_->Report(INSERT_FAILED, elapsed);
    }
    return s;
  }
  Status Delete(const std::string &table, const std::string &key) {
    if (ops_to_skip_!= 0 && ops_performed_ == 0){
      timer.Start();
    }
    timer_.Start();
    Status s = db_->Delete(table, key);
    uint64_t elapsed = timer_.End();
    if (ops_performed_++ < ops_to_skip_)
    {
      // no measurements
      if (ops_performed_ == ops_to_skip_){
        skipTime = timer.End();
      }
      return s;
    }
    if (s == kOK) {
      measurements_->Report(DELETE, elapsed);
    } else {
      measurements_->Report(DELETE_FAILED, elapsed);
    }
    return s;
  }
 
 double skipTime = 0;
 private:
  DB *db_;
  Measurements *measurements_;
  utils::Timer<uint64_t, std::nano> timer_;
  int ops_to_skip_ = 0;
  int ops_performed_ = 0;
  ycsbc::utils::Timer<double> timer;
};

} // ycsbc

#endif // YCSB_C_DB_WRAPPER_H_
