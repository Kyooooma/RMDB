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
        while (offset < len) {
            log_manager_->add();
            LogType log_type_ = *reinterpret_cast<const LogType *>(buffer_.buffer_ + offset);
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
                assert(att.count(log->log_tid_));
                att[log->log_tid_] = log->lsn_;
            } else if (log_type_ == LogType::ABORT) {
                auto log = std::make_shared<AbortLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
                log->format_print();
                assert(att.count(log->log_tid_));
                att[log->log_tid_] = log->lsn_;
            } else if (log_type_ == LogType::UPDATE) {
                auto log = std::make_shared<UpdateLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
                log->format_print();
                assert(att.count(log->log_tid_));
                att[log->log_tid_] = log->lsn_;
            } else if (log_type_ == LogType::DELETE) {
                auto log = std::make_shared<DeleteLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
                log->format_print();
                assert(att.count(log->log_tid_));
                att[log->log_tid_] = log->lsn_;
            } else if (log_type_ == LogType::INSERT) {
                auto log = std::make_shared<InsertLogRecord>();
                log->deserialize(buffer_.buffer_ + offset);
                offset += log->log_tot_len_;
                logs.push_back(log);
                log->format_print();
                assert(att.count(log->log_tid_));
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
    rollback(true);
    for(const auto& log_ : logs){
        if (auto log = std::dynamic_pointer_cast<InsertLogRecord>(log_)) {
            // redo insert
            std::cout << "redo insert\n";
            assert(sm_manager_->fhs_.count(log->table_name_));
            auto rfh = sm_manager_->fhs_[log->table_name_].get();
            try{
                rfh->insert_record(log->rid_, log->insert_value_.data);
                insert_index(&(log->insert_value_), log->rid_, log->table_name_);
            }catch (RMDBError &e){
                std::cout << "dirty_page\n";
                auto new_rid = rfh->insert_record(log->insert_value_.data, nullptr);
                assert(new_rid == log->rid_);
                insert_index(&(log->insert_value_), log->rid_, log->table_name_);
            }
        } else if (auto log = std::dynamic_pointer_cast<UpdateLogRecord>(log_)) {
            // redo update
            std::cout << "redo update\n";
            assert(sm_manager_->fhs_.count(log->table_name_));
            auto rfh = sm_manager_->fhs_[log->table_name_].get();
            delete_index(&(log->update_value_), log->table_name_);
            rfh->update_record(log->rid_, log->now_value_.data, nullptr);
            insert_index(&(log->now_value_), log->rid_, log->table_name_);
        } else if (auto log = std::dynamic_pointer_cast<DeleteLogRecord>(log_)) {
            //redo delete
            std::cout << "redo delete\n";
            assert(sm_manager_->fhs_.count(log->table_name_));
            auto rfh = sm_manager_->fhs_[log->table_name_].get();
            delete_index(&(log->delete_value_), log->table_name_);
            rfh->delete_record(log->rid_, nullptr);
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

void RecoveryManager::rollback(bool flag){
    for (auto i = att.rbegin(); i != att.rend(); i++) {
        auto [u, v] = *i;
        int now = v;
        while (now != -1) {
            if (auto log = std::dynamic_pointer_cast<InsertLogRecord>(logs[now])) {
                // 回滚insert
                assert(sm_manager_->fhs_.count(log->table_name_));
                auto rfh = sm_manager_->fhs_[log->table_name_].get();
                std::cout << "回滚insert\n";
                try{
                    delete_index(&(log->insert_value_), log->table_name_);
                    rfh->delete_record(log->rid_, nullptr);
                }catch (RMDBError &e){
                    std::cout << e.what() << '\n';
                }
                now = log->prev_lsn_;
            } else if (auto log = std::dynamic_pointer_cast<UpdateLogRecord>(logs[now])) {
                // 回滚update
                assert(sm_manager_->fhs_.count(log->table_name_));
                auto rfh = sm_manager_->fhs_[log->table_name_].get();
                std::cout << "回滚update\n";
                try{
                    delete_index(&(log->now_value_), log->table_name_);
                    rfh->update_record(log->rid_, log->update_value_.data, nullptr);
                    insert_index(&(log->now_value_), log->rid_, log->table_name_);
                }catch (RMDBError &e){
                    std::cout << e.what() << '\n';
                }
                now = log->prev_lsn_;
            } else if (auto log = std::dynamic_pointer_cast<DeleteLogRecord>(logs[now])) {
                //回滚delete
                assert(sm_manager_->fhs_.count(log->table_name_));
                auto rfh = sm_manager_->fhs_[log->table_name_].get();
                std::cout << "回滚delete\n";
                try{
                    insert_index(&(log->delete_value_), log->rid_, log->table_name_);
                    rfh->insert_record(log->rid_, log->delete_value_.data);
                }catch (RMDBError &e){
                    std::cout << e.what() << '\n';
                }
                now = log->prev_lsn_;
            } else if (auto log = std::dynamic_pointer_cast<BeginLogRecord>(logs[now])) {
                now = log->prev_lsn_;
            } else if (auto log = std::dynamic_pointer_cast<AbortLogRecord>(logs[now])) {
                if(flag){
                    now = log->prev_lsn_;
                }else{
                    break;
                }
            } else if (auto log = std::dynamic_pointer_cast<CommitLogRecord>(logs[now])) {
                if(flag){
                    now = log->prev_lsn_;
                }else{
                    break;
                }
            } else {
                std::cout << "undo 不太对\n";
            }
        }
    }
}

void RecoveryManager::delete_index(RmRecord* rec, std::string tab_name_){
    // 删除索引
    auto tab_ = sm_manager_->db_.get_table(tab_name_);
    for (auto &index: tab_.indexes) {
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
        char *key = new char[index.col_tot_len];
        int offset = 0;
        for (size_t j = 0; j < index.col_num; ++j) {
            memcpy(key + offset, rec->data + index.cols[j].offset, index.cols[j].len);
            offset += index.cols[j].len;
        }
        ih->delete_entry(key, nullptr);
        free(key);
    }
}

bool RecoveryManager::insert_index(RmRecord* rec, Rid rid_, std::string tab_name_){
    // 插入索引
    auto tab_ = sm_manager_->db_.get_table(tab_name_);
    int fail_p = -1;
    for (int i = 0; i < tab_.indexes.size(); i++) {
        auto &index = tab_.indexes[i];
        auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
        char *key = new char[index.col_tot_len];
        int offset = 0;
        for (size_t j = 0; j < index.col_num; ++j) {
            memcpy(key + offset, rec->data + index.cols[j].offset, index.cols[j].len);
            offset += index.cols[j].len;
        }
        auto result = ih->insert_entry(key, rid_, nullptr);
        free(key);
        if(!result.second){
            fail_p = i;
            break;
        }
    }
    if(fail_p != -1){
        //说明插入失败，需要rollback
        //删掉已插入索引
        for(int i = 0; i < fail_p; i++){
            auto &index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            char *key = new char[index.col_tot_len];
            int offset = 0;
            for (size_t j = 0; j < index.col_num; ++j) {
                memcpy(key + offset, rec->data + index.cols[j].offset, index.cols[j].len);
                offset += index.cols[j].len;
            }
            ih->delete_entry(key, nullptr);
            free(key);
        }
        return false;
    }
    return true;
}