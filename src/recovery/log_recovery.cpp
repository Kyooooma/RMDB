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
    std::unordered_set<std::string> tables;
    while (true) {
        int len = disk_manager_->read_log(buffer_.buffer_, LOG_BUFFER_SIZE, off);
        if (len == -1 || len == 0) break;
        int offset = 0;
        while (offset < len) {
            if(offset + OFFSET_LOG_TOT_LEN > len){
                std::cout << offset << " " << OFFSET_LOG_TOT_LEN << " " << len << "\n";
                break;
            }
            auto log_tot_len_ = *reinterpret_cast<const uint32_t*>(buffer_.buffer_ + offset + OFFSET_LOG_TOT_LEN);
            if(offset + log_tot_len_ > len) {
                std::cout << offset << " " << log_tot_len_ << " " << len << "\n";
                break;
            }
            LogType log_type_ = *reinterpret_cast<const LogType *>(buffer_.buffer_ + offset);
            if (log_type_ == LogType::begin) {
                auto log = std::make_shared<BeginLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
//                log->format_print();
                att[log->log_tid_] = log->lsn_;
            } else if (log_type_ == LogType::commit) {
                auto log = std::make_shared<CommitLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
//                log->format_print();
                assert(att.count(log->log_tid_));
                att[log->log_tid_] = log->lsn_;
            } else if (log_type_ == LogType::ABORT) {
                auto log = std::make_shared<AbortLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
//                log->format_print();
                assert(att.count(log->log_tid_));
                att[log->log_tid_] = log->lsn_;
            } else if (log_type_ == LogType::UPDATE) {
                auto log = std::make_shared<UpdateLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
//                log->format_print();
                assert(att.count(log->log_tid_));
                att[log->log_tid_] = log->lsn_;
                tables.insert(log->table_name_);
            } else if (log_type_ == LogType::DELETE) {
                auto log = std::make_shared<DeleteLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
//                log->format_print();
                assert(att.count(log->log_tid_));
                att[log->log_tid_] = log->lsn_;
                tables.insert(log->table_name_);
            } else if (log_type_ == LogType::INSERT) {
                auto log = std::make_shared<InsertLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
//                log->format_print();
                assert(att.count(log->log_tid_));
                att[log->log_tid_] = log->lsn_;
                tables.insert(log->table_name_);
            } else if (log_type_ == LogType::INDEX_INSERT) {
                auto log = std::make_shared<IndexInsertLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
//                log->format_print();
                assert(att.count(log->log_tid_));
                att[log->log_tid_] = log->lsn_;
            } else if (log_type_ == LogType::INDEX_DELETE) {
                auto log = std::make_shared<IndexDeleteLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
//                log->format_print();
                assert(att.count(log->log_tid_));
                att[log->log_tid_] = log->lsn_;
            } else {
                std::cout << "huai le\n" << log_type_;
                exit(0);
            }
        }
        off += offset;
    }
    //清空索引并重建
    for(auto &i : tables){
        auto &tab = sm_manager_->db_.get_table(i);
        for(const auto& index : tab.indexes){
            auto ix_name = sm_manager_->get_ix_manager()->get_index_name(tab.name, index.cols);
            if (sm_manager_->ihs_.count(ix_name)) {// 说明被打开了
                disk_manager_->close_file(sm_manager_->ihs_[ix_name]->get_fd());
                sm_manager_->ihs_.erase(ix_name);
            }
            sm_manager_->get_ix_manager()->destroy_index(tab.name, index.cols);
            sm_manager_->get_ix_manager()->create_index(tab.name, index.cols);
            sm_manager_->ihs_.emplace(ix_name, sm_manager_->get_ix_manager()->open_index(tab.name, index.cols));
        }
    }
}

/**
 * @description: 重做所有未落盘的操作
 */
void RecoveryManager::redo() {
    rollback(true);
    for (const auto &log_: logs) {
        if (auto log = std::dynamic_pointer_cast<InsertLogRecord>(log_)) {
            // redo insert
            std::cout << "redo insert\n";
            assert(sm_manager_->fhs_.count(log->table_name_));
            auto rfh = sm_manager_->fhs_[log->table_name_].get();
            try {
                rfh->insert_record(log->rid_, log->insert_value_.data);
            } catch (RMDBError &e) {
                std::cout << "dirty_page\n";
                auto new_rid = rfh->insert_record(log->insert_value_.data, nullptr);
                assert(new_rid == log->rid_);
            }
        } else if (auto log = std::dynamic_pointer_cast<UpdateLogRecord>(log_)) {
            // redo update
            std::cout << "redo update\n";
            assert(sm_manager_->fhs_.count(log->table_name_));
            auto rfh = sm_manager_->fhs_[log->table_name_].get();
            rfh->update_record(log->rid_, log->now_value_.data, nullptr);
        } else if (auto log = std::dynamic_pointer_cast<DeleteLogRecord>(log_)) {
            //redo delete
            std::cout << "redo delete\n";
            assert(sm_manager_->fhs_.count(log->table_name_));
            auto rfh = sm_manager_->fhs_[log->table_name_].get();
            rfh->delete_record(log->rid_, nullptr);
        } else if (auto log = std::dynamic_pointer_cast<IndexInsertLogRecord>(log_)) {
            //redo index insert
            std::cout << "redo index insert\n";
            assert(sm_manager_->ihs_.count(log->ix_name_));
            auto ih = sm_manager_->ihs_.at(log->ix_name_).get();
            std::cout << ih->insert_entry(log->key_, log->rid_, nullptr).second << '\n';
        } else if (auto log = std::dynamic_pointer_cast<IndexDeleteLogRecord>(log_)) {
            //redo index delete
            std::cout << "redo index delete\n";
            assert(sm_manager_->ihs_.count(log->ix_name_));
            auto ih = sm_manager_->ihs_.at(log->ix_name_).get();
            std::cout << ih->delete_entry(log->key_, nullptr) << '\n';
        } else if (auto log = std::dynamic_pointer_cast<BeginLogRecord>(log_)) {
            continue;
        } else if (auto log = std::dynamic_pointer_cast<AbortLogRecord>(log_)) {
            continue;
        } else if (auto log = std::dynamic_pointer_cast<CommitLogRecord>(log_)) {
            continue;
        } else {
            std::cout << "redo error\n";
        }
    }
}

/**
 * @description: 回滚未完成的事务
 */
void RecoveryManager::undo() {
    rollback(false);
}

void RecoveryManager::rollback(bool flag) {
    for (auto i = att.rbegin(); i != att.rend(); i++) {
        auto [u, v] = *i;
        int now = v;
        while (now != -1) {
            if (auto log = std::dynamic_pointer_cast<InsertLogRecord>(logs[now])) {
                // 回滚insert
                assert(sm_manager_->fhs_.count(log->table_name_));
                auto rfh = sm_manager_->fhs_[log->table_name_].get();
                std::cout << "回滚insert\n";
                try {
                    rfh->delete_record(log->rid_, nullptr);
                } catch (RMDBError &e) {
                    std::cout << e.what() << '\n';
                }
                now = log->prev_lsn_;
            } else if (auto log = std::dynamic_pointer_cast<UpdateLogRecord>(logs[now])) {
                // 回滚update
                assert(sm_manager_->fhs_.count(log->table_name_));
                auto rfh = sm_manager_->fhs_[log->table_name_].get();
                std::cout << "回滚update\n";
                try {
                    rfh->update_record(log->rid_, log->update_value_.data, nullptr);
                } catch (RMDBError &e) {
                    std::cout << e.what() << '\n';
                }
                now = log->prev_lsn_;
            } else if (auto log = std::dynamic_pointer_cast<DeleteLogRecord>(logs[now])) {
                //回滚delete
                assert(sm_manager_->fhs_.count(log->table_name_));
                auto rfh = sm_manager_->fhs_[log->table_name_].get();
                std::cout << "回滚delete\n";
                try {
                    rfh->insert_record(log->rid_, log->delete_value_.data);
                } catch (RMDBError &e) {
                    std::cout << e.what() << '\n';
                }
                now = log->prev_lsn_;
            } else if (auto log = std::dynamic_pointer_cast<IndexInsertLogRecord>(logs[now])) {
                //回滚index insert
                if(!flag){
                    assert(sm_manager_->ihs_.count(log->ix_name_));
                    auto ih = sm_manager_->ihs_.at(log->ix_name_).get();
                    std::cout << "回滚index insert\n" <<
                              ih->delete_entry(log->key_, nullptr) << '\n';
                }
                now = log->prev_lsn_;
            } else if (auto log = std::dynamic_pointer_cast<IndexDeleteLogRecord>(logs[now])) {
                //回滚index delete
                if(!flag){
                    assert(sm_manager_->ihs_.count(log->ix_name_));
                    auto ih = sm_manager_->ihs_.at(log->ix_name_).get();
                    std::cout << "回滚index delete\n" <<
                              ih->insert_entry(log->key_, log->rid_, nullptr).second << '\n';
                }
                now = log->prev_lsn_;
            } else if (auto log = std::dynamic_pointer_cast<BeginLogRecord>(logs[now])) {
                now = log->prev_lsn_;
            } else if (auto log = std::dynamic_pointer_cast<AbortLogRecord>(logs[now])) {
                if (flag) {
                    now = log->prev_lsn_;
                } else {
                    break;
                }
            } else if (auto log = std::dynamic_pointer_cast<CommitLogRecord>(logs[now])) {
                if (flag) {
                    now = log->prev_lsn_;
                } else {
                    break;
                }
            } else {
                std::cout << "undo 不太对\n";
            }
        }
    }
}