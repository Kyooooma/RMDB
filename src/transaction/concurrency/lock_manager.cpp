/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "lock_manager.h"

/**
 * @description: 申请行级共享锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID 记录所在的表的fd
 * @param {int} tab_fd
 */
bool LockManager::lock_shared_on_record(Transaction *txn, const Rid &rid, int tab_fd) {
    std::unique_lock<std::mutex> lock{ latch_ };
    txn->set_state(TransactionState::GROWING);
    // 查看是否持有表级锁
    LockDataId data_id_table = LockDataId(tab_fd, LockDataType::TABLE);
    LockDataId data_id_record = LockDataId(tab_fd, rid, LockDataType::RECORD);
    bool is_ok_table = true;
    while(true){
        auto iter = lock_table_.find(data_id_table);
        if(iter != lock_table_.end()){
            std::list<LockRequest>& request_queue = iter->second->request_queue_;
            for(auto it : request_queue){
                if(it.granted_){// 持有锁
                    if(it.txn_id_ == txn->get_transaction_id()){
                        // 本人持有锁
                        return true;
                    }
                    // 非本人持有写锁
                    if(it.lock_mode_ == LockMode::EXLUCSIVE){
                        is_ok_table = false;
                        if(it.txn_id_ < txn->get_transaction_id()){
                            // 申请锁的事务更年轻
                            throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                        }
                    }
                }
            }
        }
        auto iter_record = lock_table_.find(data_id_record);
        if(iter_record == lock_table_.end()){
            // 第一次申请锁, 说明行上未加锁
            std::unique_ptr<LockRequestQueue> record_lock_request_queue = std::make_unique<LockRequestQueue>();
            record_lock_request_queue->group_lock_mode_ = GroupLockMode::S;
            record_lock_request_queue->request_queue_.emplace_back(txn->get_transaction_id(), LockMode::SHARED, is_ok_table);
            lock_table_[data_id_record] = std::move(record_lock_request_queue);
            if(is_ok_table){
                // 说明表锁未起冲突
                txn->lock_set_->insert(data_id_record);
                return true;
            }
        }else if(is_ok_table){
            // 说明表锁未起冲突
            std::list<LockRequest>& request_queue = iter->second->request_queue_;
            LockRequest * request = nullptr;
            bool ok = true;
            for(auto &it : request_queue){
                if(it.txn_id_ == txn->get_transaction_id()){
                    request = &it;
                    if(it.granted_) return true;
                }
                if(it.granted_ && it.lock_mode_ == LockMode::EXLUCSIVE){
                    ok = false;
                    if(it.txn_id_ < txn->get_transaction_id()){
                        // 申请锁的事务更年轻
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                    }
                }
            }
            if(request == nullptr){
                request_queue.emplace_back(txn->get_transaction_id(), LockMode::SHARED, false);
                request = &(request_queue.back());
            }
            if(ok){
                request->granted_ = true;
                txn->lock_set_->insert(data_id_record);
                return true;
            }
        }
        iter_record->second->cv_.wait(lock);
    }
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction *txn, const Rid &rid, int tab_fd) {
    std::unique_lock<std::mutex> lock{ latch_ };
    txn->set_state(TransactionState::GROWING);
    // 查看是否持有表级锁
    LockDataId data_id_table = LockDataId(tab_fd, LockDataType::TABLE);
    LockDataId data_id_record = LockDataId(tab_fd, rid, LockDataType::RECORD);
    bool is_ok_table = true;
    while(true){
        auto iter = lock_table_.find(data_id_table);
        if(iter != lock_table_.end()){
            std::list<LockRequest>& request_queue = iter->second->request_queue_;
            LockRequest * request = nullptr;
            for(auto &it : request_queue){
                if(it.granted_){// 持有锁
                    if(it.txn_id_ == txn->get_transaction_id()){
                        // 本人持有锁
                        request = &it;
                        if(it.lock_mode_ == LockMode::EXLUCSIVE){
                            //本人持有表级写锁
                            return true;
                        }
                        continue;
                    }
                    // 非本人持有锁
                    is_ok_table = false;
                    if(it.txn_id_ < txn->get_transaction_id()){
                        // 申请锁的事务更年轻
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                    }
                }
            }
            if(request != nullptr && is_ok_table){
                //本人持有表级读锁且没有其他锁
                request->lock_mode_ = LockMode::EXLUCSIVE; // 锁升级
                return true;
            }
        }
        auto iter_record = lock_table_.find(data_id_record);
        if(iter_record == lock_table_.end()){
            // 第一次申请锁, 说明行上未加锁
            std::unique_ptr<LockRequestQueue> record_lock_request_queue = std::make_unique<LockRequestQueue>();
            record_lock_request_queue->group_lock_mode_ = GroupLockMode::X;
            record_lock_request_queue->request_queue_.emplace_back(txn->get_transaction_id(), LockMode::EXLUCSIVE, is_ok_table);
            lock_table_[data_id_record] = std::move(record_lock_request_queue);
            if(is_ok_table){
                // 说明表锁未起冲突
                txn->lock_set_->insert(data_id_record);
                return true;
            }
        }else if(is_ok_table){
            // 说明表锁未起冲突
            std::list<LockRequest>& request_queue = iter->second->request_queue_;
            LockRequest * request = nullptr;
            bool ok = true;
            for(auto &it : request_queue){
                if(it.txn_id_ == txn->get_transaction_id()){
                    request = &it;
                    if(it.granted_ && it.lock_mode_ == LockMode::EXLUCSIVE){
                        //本人持有行级写锁
                        return true;
                    }
                    continue;
                }
                if(it.granted_){
                    //非本人持有锁
                    ok = false;
                    if(it.txn_id_ < txn->get_transaction_id()){
                        // 申请锁的事务更年轻
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                    }
                }
            }
            if(request == nullptr){
                request_queue.emplace_back(txn->get_transaction_id(), LockMode::EXLUCSIVE, false);
                request = &(request_queue.back());
            }
            if(ok){
                request->granted_ = true;
                request->lock_mode_ = LockMode::EXLUCSIVE;// 锁升级
                txn->lock_set_->insert(data_id_record);
                return true;
            }
        }
        iter_record->second->cv_.wait(lock);
    }
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction *txn, int tab_fd) {
    std::unique_lock<std::mutex> lock{ latch_ };
    txn->set_state(TransactionState::GROWING);
    // 查看是否持有表级锁
    LockDataId data_id_table = LockDataId(tab_fd, LockDataType::TABLE);
    while(true){
        auto iter = lock_table_.find(data_id_table);
        if(iter == lock_table_.end()){
            // 第一次申请锁, 说明表上未加锁
            std::unique_ptr<LockRequestQueue> record_lock_request_queue = std::make_unique<LockRequestQueue>();
            record_lock_request_queue->group_lock_mode_ = GroupLockMode::S;
            record_lock_request_queue->request_queue_.emplace_back(txn->get_transaction_id(), LockMode::SHARED, true);
            lock_table_[data_id_table] = std::move(record_lock_request_queue);
            txn->lock_set_->insert(data_id_table);
            return true;
        }else{
            std::list<LockRequest>& request_queue = iter->second->request_queue_;
            LockRequest * request = nullptr;
            bool ok = true;
            for(auto &it : request_queue){
                if(it.txn_id_ == txn->get_transaction_id()){
                    request = &it;
                    if(it.granted_) return true;
                }
                if(it.granted_ && it.lock_mode_ == LockMode::EXLUCSIVE){
                    //其他人持有写锁
                    ok = false;
                    if(it.txn_id_ < txn->get_transaction_id()){
                        // 申请锁的事务更年轻
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                    }
                }
            }
            if(request == nullptr){
                request_queue.emplace_back(txn->get_transaction_id(), LockMode::SHARED, false);
                request = &(request_queue.back());
            }
            if(ok){
                request->granted_ = true;
                txn->lock_set_->insert(data_id_table);
                return true;
            }
        }
        iter->second->cv_.wait(lock);
    }
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction *txn, int tab_fd) {
    std::unique_lock<std::mutex> lock{ latch_ };
    txn->set_state(TransactionState::GROWING);
    // 查看是否持有表级锁
    LockDataId data_id_table = LockDataId(tab_fd, LockDataType::TABLE);
    while(true){
        auto iter = lock_table_.find(data_id_table);
        if(iter == lock_table_.end()){
            // 第一次申请锁, 说明表上未加锁
            std::unique_ptr<LockRequestQueue> record_lock_request_queue = std::make_unique<LockRequestQueue>();
            record_lock_request_queue->group_lock_mode_ = GroupLockMode::X;
            record_lock_request_queue->request_queue_.emplace_back(txn->get_transaction_id(), LockMode::EXLUCSIVE, true);
            lock_table_[data_id_table] = std::move(record_lock_request_queue);
            txn->lock_set_->insert(data_id_table);
            return true;
        }else{
            std::list<LockRequest>& request_queue = iter->second->request_queue_;
            LockRequest * request = nullptr;
            bool ok = true;
            for(auto &it : request_queue){
                if(it.txn_id_ == txn->get_transaction_id()){
                    request = &it;
                    if(it.granted_ && it.lock_mode_ == LockMode::EXLUCSIVE) return true;
                    continue;
                }
                if(it.granted_){
                    //其他人持有锁
                    ok = false;
                    if(it.txn_id_ < txn->get_transaction_id()){
                        // 申请锁的事务更年轻
                        throw TransactionAbortException(txn->get_transaction_id(), AbortReason::DEADLOCK_PREVENTION);
                    }
                }
            }
            if(request == nullptr){
                request_queue.emplace_back(txn->get_transaction_id(), LockMode::EXLUCSIVE, false);
                request = &(request_queue.back());
            }
            if(ok){
                request->granted_ = true;
                request->lock_mode_ = LockMode::EXLUCSIVE; // 锁升级
                txn->lock_set_->insert(data_id_table);
                return true;
            }
        }
        iter->second->cv_.wait(lock);
    }
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction *txn, int tab_fd) {
    return true;
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction *txn, int tab_fd) {
    return true;
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction *txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock{ latch_ };
    txn->set_state(TransactionState::SHRINKING);

    auto iter = lock_table_.find(lock_data_id);
    if(iter == lock_table_.end()){
        return false;
    }
    std::list<LockRequest>& request_queue = iter->second->request_queue_;
    auto it = request_queue.begin();
    bool ok = true;
    while(it != request_queue.end()){
        if(it->txn_id_ == txn->get_transaction_id()){
            it = request_queue.erase(it);
        }else{
            it++;
            ok = false;
        }
    }
    if(ok){
        lock_table_.erase(lock_data_id);
    }else{
        iter->second->cv_.notify_all();
    }
    return true;
}