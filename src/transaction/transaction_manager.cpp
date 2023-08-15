/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "transaction_manager.h"
#include "record/rm_file_handle.h"
#include "system/sm_manager.h"

std::unordered_map<txn_id_t, Transaction *> TransactionManager::txn_map = {};

/**
 * @description: 事务的开始方法
 * @return {Transaction*} 开始事务的指针
 * @param {Transaction*} txn 事务指针，空指针代表需要创建新事务，否则开始已有事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
Transaction * TransactionManager::begin(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 判断传入事务参数是否为空指针
    // 2. 如果为空指针，创建新事务
    // 3. 把开始事务加入到全局事务表中
    // 4. 返回当前事务指针
    if(txn == nullptr){
        txn = new Transaction(get_next_txn_id());
    }
    std::unique_lock<std::mutex> lock(latch_);
    txn_map.emplace(txn->get_transaction_id(), txn);
    auto *log = new BeginLogRecord(txn->get_transaction_id());
    log->prev_lsn_ = txn->get_prev_lsn();
    log_manager->add_log_to_buffer(log);
    txn->set_prev_lsn(log->lsn_);
    delete log;
    return txn;
}

/**
 * @description: 事务的提交方法
 * @param {Transaction*} txn 需要提交的事务
 * @param {LogManager*} log_manager 日志管理器指针
 */
void TransactionManager::commit(Transaction* txn, LogManager* log_manager) {
    // Todo:
    // 1. 如果存在未提交的写操作，提交所有的写操作
    // 2. 释放所有锁
    // 3. 释放事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    //释放所有锁
    //释放事务相关资源，eg.锁集
    auto lock_set = txn->get_lock_set();
    for(auto i : *lock_set){
        lock_manager_->unlock(txn, i);
    }
    txn->clear();
    // 4. 把事务日志刷入磁盘中
    auto *log = new CommitLogRecord(txn->get_transaction_id());
    log->prev_lsn_ = txn->get_prev_lsn();
    log_manager->add_log_to_buffer(log);
    txn->set_prev_lsn(log->lsn_);
    delete log;
    // 5. 更新事务状态
    txn->set_state(TransactionState::COMMITTED);
}

void TransactionManager::delete_index(const std::string& tab_name, RmRecord* rec, Rid rid_, Context* context_){
    // 删除索引
    auto &tab = sm_manager_->db_.get_table(tab_name);
    for (auto &index: tab.indexes) {
        auto ix_name = sm_manager_->get_ix_manager()->get_index_name(tab_name, index.cols);
        auto ih = sm_manager_->ihs_.at(ix_name).get();
        char *key = new char[index.col_tot_len];
        int offset = 0;
        for (size_t j = 0; j < index.col_num; ++j) {
            memcpy(key + offset, rec->data + index.cols[j].offset, index.cols[j].len);
            offset += index.cols[j].len;
        }

        //更新索引删除日志
        auto *index_log = new IndexDeleteLogRecord(context_->txn_->get_transaction_id(), key, rid_, ix_name, index.col_tot_len);
        index_log->prev_lsn_ = context_->txn_->get_prev_lsn();
        context_->log_mgr_->add_log_to_buffer(index_log);
        context_->txn_->set_prev_lsn(index_log->lsn_);
        delete index_log;
        ih->delete_entry(key, nullptr);
        free(key);
    }
}

void TransactionManager::insert_index(const std::string& tab_name, RmRecord* rec, Rid rid_, Context* context_){
    // 插入索引
    auto &tab = sm_manager_->db_.get_table(tab_name);
    for (auto & index : tab.indexes) {
        auto ix_name = sm_manager_->get_ix_manager()->get_index_name(tab_name, index.cols);
        auto ih = sm_manager_->ihs_.at(ix_name).get();
        char *key = new char[index.col_tot_len];
        int offset = 0;
        for (size_t j = 0; j < index.col_num; ++j) {
            memcpy(key + offset, rec->data + index.cols[j].offset, index.cols[j].len);
            offset += index.cols[j].len;
        }

        //更新索引插入日志
        auto *index_log = new IndexInsertLogRecord(context_->txn_->get_transaction_id(), key, rid_, ix_name, index.col_tot_len);
        index_log->prev_lsn_ = context_->txn_->get_prev_lsn();
        context_->log_mgr_->add_log_to_buffer(index_log);
        context_->txn_->set_prev_lsn(index_log->lsn_);
        delete index_log;
        auto result = ih->insert_entry(key, rid_, nullptr);
        assert(result.second == true);
        free(key);
    }
}

/**
 * @description: 事务的终止（回滚）方法
 * @param {Transaction *} txn 需要回滚的事务
 * @param {LogManager} *log_manager 日志管理器指针
 */
void TransactionManager::abort(Context * context, LogManager *log_manager) {
    // Todo:
    // 1. 回滚所有写操作
    // 2. 释放所有锁
    // 3. 清空事务相关资源，eg.锁集
    // 4. 把事务日志刷入磁盘中
    // 5. 更新事务状态
    auto txn = context->txn_;
    auto write_set = txn->get_write_set();
    //从后往前遍历
    while(!write_set->empty()){
        auto last = write_set->back();
        write_set->pop_back();
        auto type = last->GetWriteType();
        auto rid = last->GetRid();
        auto tab_name = last->GetTableName();
        auto rec = last->GetRecord();
        assert(sm_manager_->fhs_.count(tab_name));
        auto rfh = sm_manager_->fhs_[tab_name].get();
        if(type == WType::INSERT_TUPLE){
            //插入操作, 应该删除
//            std::cout << "rollback insert\n";

            //更新日志
            auto *logRecord = new DeleteLogRecord(context->txn_->get_transaction_id(), rec, rid,tab_name);
            logRecord->prev_lsn_ = context->txn_->get_prev_lsn();
            context->log_mgr_->add_log_to_buffer(logRecord);
            context->txn_->set_prev_lsn(logRecord->lsn_);
            delete logRecord;
            delete_index(tab_name, &rec, rid, context);
            rfh->delete_record(rid, context);
        }else if(type == WType::DELETE_TUPLE){
            //删除操作, 应该插入
//            std::cout << "rollback delete\n";
            //更新日志-插入
            auto *logRecord = new InsertLogRecord(context->txn_->get_transaction_id(), rec, rid,tab_name);
            logRecord->prev_lsn_ = context->txn_->get_prev_lsn();
            context->log_mgr_->add_log_to_buffer(logRecord);
            context->txn_->set_prev_lsn(logRecord->lsn_);
            delete logRecord;
            insert_index(tab_name, &rec, rid, context);
            rfh->insert_record(rid, rec.data);
        }else if(type == WType::UPDATE_TUPLE){
            //更新操作, 应该更新
//            std::cout << "rollback update\n";
            auto old = rfh->get_record(rid, context);

            //更新日志
            auto *logRecord = new UpdateLogRecord(context->txn_->get_transaction_id(), *old, rid,tab_name, rec);
            logRecord->prev_lsn_ = context->txn_->get_prev_lsn();
            context->log_mgr_->add_log_to_buffer(logRecord);
            context->txn_->set_prev_lsn(logRecord->lsn_);
            delete logRecord;
            delete_index(tab_name, old.get(), rid, context);
            rfh->update_record(rid, rec.data, context);
            insert_index(tab_name, &rec, rid, context);
        }
    }
    //释放所有锁
    auto lock_set = txn->get_lock_set();
    for(auto i : *lock_set){
        lock_manager_->unlock(txn, i);
    }
    //释放事务相关资源，eg.锁集
    txn->clear();
    // 4. 把事务日志刷入磁盘中
    auto *log = new AbortLogRecord(txn->get_transaction_id());
    log->prev_lsn_ = txn->get_prev_lsn();
    log_manager->add_log_to_buffer(log);
    txn->set_prev_lsn(log->lsn_);
    delete log;
    // 5. 更新事务状态
    txn->set_state(TransactionState::ABORTED);
}