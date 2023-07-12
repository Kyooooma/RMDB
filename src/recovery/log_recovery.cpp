/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "log_recovery.h"

/**
 * @description: analyze阶段，需要获得脏页表（DPT）和未完成的事务列表（ATT）
 */
void RecoveryManager::analyze() {
    int off = 0;
    while (true) {
        int len = disk_manager_->read_log(buffer_.buffer_, LOG_BUFFER_SIZE, off);
        if (len == -1 || len == 0) return;
        off += len;
        int offset = 0;
        while (offset < len - 1) {
            LogType log_type_ = *reinterpret_cast<const LogType *>(buffer_.buffer_ + offset);
            log_manager_->global();
            if (log_type_ == LogType::begin) {
                auto log = std::make_shared<BeginLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
                log->format_print();
                att[log->log_tid_] = log->lsn_;
            } else if (log_type_ == LogType::commit) {
                auto log = std::make_shared<CommitLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
                log->format_print();
                att.erase(log->log_tid_);
            } else if (log_type_ == LogType::ABORT) {
                auto log = std::make_shared<AbortLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
                log->format_print();
                att.erase(log->log_tid_);
            } else if (log_type_ == LogType::UPDATE) {
                auto log = std::make_shared<UpdateLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
                log->format_print();
                att[log->log_tid_] = log->lsn_;
            } else if (log_type_ == LogType::DELETE) {
                auto log = std::make_shared<DeleteLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
                std::cout << logs.back()->log_type_ << " " << logs.size() <<'\n';
                log->format_print();
                att[log->log_tid_] = log->lsn_;
            } else if (log_type_ == LogType::INSERT) {
                auto log = std::make_shared<InsertLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
                log->format_print();
                att[log->log_tid_] = log->lsn_;
            } else {
                std::cout << "huai le\n";
            }
        }
    }

}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo() {

}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    for (auto [u, v]: att) {
        int now = v;
        while (now != -1) {
            if (auto log = std::dynamic_pointer_cast<InsertLogRecord>(logs[now])) {
                // 回滚insert
                std::cout << log->table_name_ << '\n';
                assert(sm_manager_->fhs_.count(log->table_name_));
                auto rfh = sm_manager_->fhs_[log->table_name_].get();
                std::cout << "回滚insert\n";
                rfh->delete_record(log->rid_, nullptr);
                now = log->prev_lsn_;
            } else if (auto log = std::dynamic_pointer_cast<UpdateLogRecord>(logs[now])) {
                // 回滚update
                assert(sm_manager_->fhs_.count(log->table_name_));
                auto rfh = sm_manager_->fhs_[log->table_name_].get();
                std::cout << "回滚update\n";
                rfh->delete_record(log->rid_, nullptr);
                rfh->insert_record(log->rid_,log->update_value_.data);
                now = log->prev_lsn_;
            } else if (auto log = std::dynamic_pointer_cast<DeleteLogRecord>(logs[now])) {
                //回滚delete
                assert(sm_manager_->fhs_.count(log->table_name_));
                auto rfh = sm_manager_->fhs_[log->table_name_].get();
                std::cout << "回滚delete\n";
                rfh->insert_record(log->rid_, log->delete_value_.data);
                now = log->prev_lsn_;
            } else if (auto log = std::dynamic_pointer_cast<BeginLogRecord>(logs[now])) {
                std::cout << "begin\n";
                break;
            } else if (auto log = std::dynamic_pointer_cast<CommitLogRecord>(logs[now])){
                std::cout << log->log_type_ << '\n';
                std::cout << "commit\n";
                break;
            } else if (auto log = std::dynamic_pointer_cast<AbortLogRecord>(logs[now])) {
                std::cout << "abort\n";
                break;
            } else {
                std::cout << now << "huai le\n";
                return;
            }
        }
    }
}