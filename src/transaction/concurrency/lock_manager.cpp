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
bool LockManager::lock_shared_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::cout << txn->get_transaction_id() << "申请S锁, rid:" << rid.slot_no << ", " << rid.page_no << ", tab_fd" << tab_fd << '\n';
    std::unique_lock<std::mutex> lock(latch_);
    txn->set_state(TransactionState::GROWING);
    while (true) {
        LockDataId lock_data_id_table = {tab_fd, LockDataType::TABLE};
        auto &lock_request_queue = lock_table_[lock_data_id_table];

        // 表上有X锁不能申请
        if (lock_request_queue.group_lock_mode_ == GroupLockMode::X) {
            lock.unlock();
            sleep(1);
            lock.lock();
            continue;
        }

        // 行上有X锁不能申请
        LockDataId lock_data_id = {tab_fd, rid, LockDataType::RECORD};
        auto &request_queue = lock_table_[lock_data_id];
        for (auto &request: request_queue.request_queue_) {
            if (request.lock_mode_ == LockMode::EXLUCSIVE && request.txn_id_ != txn->get_transaction_id()) {
                lock.unlock();
                sleep(1);
                lock.lock();
                continue;
            }
        }
        // 行上加S锁
        LockRequest lock_request = {txn->get_transaction_id(), LockMode::SHARED};
        if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
            request_queue.group_lock_mode_ = GroupLockMode::S;
        }
        for (auto &request : request_queue.request_queue_) {
            if (request.txn_id_ == txn->get_transaction_id()) {
                return true;
            }
        }
        request_queue.request_queue_.push_back(lock_request);
        txn->set_lock_set(lock_data_id);
        return true;
    }
}

/**
 * @description: 申请行级排他锁
 * @return {bool} 加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {Rid&} rid 加锁的目标记录ID
 * @param {int} tab_fd 记录所在的表的fd
 */
bool LockManager::lock_exclusive_on_record(Transaction* txn, const Rid& rid, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    txn->set_state(TransactionState::GROWING);
    std::cout << txn->get_transaction_id() << "申请X锁, rid:" << rid.slot_no << ", " << rid.page_no << ", tab_fd" << tab_fd << '\n';
    while (true) {
        LockDataId lock_data_id_table = {tab_fd, LockDataType::TABLE};
        auto &lock_request_queue = lock_table_[lock_data_id_table];

        // 表上有X锁不能申请
        if (lock_request_queue.group_lock_mode_ == GroupLockMode::X) {
            lock.unlock();
            sleep(1);
            lock.lock();
            continue;
        }

        // 表上有S锁不能申请
        if (lock_request_queue.group_lock_mode_ == GroupLockMode::S) {
            lock.unlock();
            sleep(1);
            lock.lock();
            continue;
        }

        // 表上有SIX锁不能申请
        if (lock_request_queue.group_lock_mode_ == GroupLockMode::SIX) {
            lock.unlock();
            sleep(1);
            lock.lock();
            continue;
        }

        // 行上有S锁不能申请
        LockDataId lock_data_id = {tab_fd, rid, LockDataType::RECORD};
        auto &request_queue = lock_table_[lock_data_id];
        for (auto &request: request_queue.request_queue_) {
            if (request.lock_mode_ == LockMode::SHARED && request.txn_id_ != txn->get_transaction_id()) {
                lock.unlock();
                sleep(1);
                lock.lock();
                continue;
            }
        }
        // 行上有X锁不能申请
        for (auto &request: request_queue.request_queue_) {
            if (request.lock_mode_ == LockMode::EXLUCSIVE && request.txn_id_ != txn->get_transaction_id()) {
                lock.unlock();
                sleep(1);
                lock.lock();
                continue;
            }
        }
        // 行上加X锁
        LockRequest lock_request = {txn->get_transaction_id(), LockMode::EXLUCSIVE};
        request_queue.group_lock_mode_ = GroupLockMode::X;
        for (auto &request : request_queue.request_queue_) {
            if (request.txn_id_ == txn->get_transaction_id()) {
                request.lock_mode_ = LockMode::EXLUCSIVE;
                return true;
            }
        }
        request_queue.request_queue_.push_back(lock_request);
        txn->set_lock_set(lock_data_id);
        return true;
    }
}

/**
 * @description: 申请表级读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_shared_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    txn->set_state(TransactionState::GROWING);
    while (true) {
        LockDataId lock_data_id_table = {tab_fd, LockDataType::TABLE};
        auto &lock_request_queue = lock_table_[lock_data_id_table];

        // 表上有IX锁不能申请
        if (lock_request_queue.group_lock_mode_ == GroupLockMode::IX ||
            lock_request_queue.group_lock_mode_ == GroupLockMode::SIX) {
            lock.unlock();
            sleep(1);
            lock.lock();
            continue;
        }

        // 表上有X锁不能申请
        if (lock_request_queue.group_lock_mode_ == GroupLockMode::X) {
            lock.unlock();
            sleep(1);
            lock.lock();
            continue;
        }

        // 表上加S锁
        LockDataId lock_data_id = {tab_fd, LockDataType::TABLE};
        auto &request_queue = lock_table_[lock_data_id];
        LockRequest lock_request = {txn->get_transaction_id(), LockMode::SHARED};
        if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK) request_queue.group_lock_mode_ = GroupLockMode::S;
        for (auto &request : request_queue.request_queue_) {
            if (request.txn_id_ == txn->get_transaction_id()) {
                return true;
            }
        }
        request_queue.request_queue_.push_back(lock_request);
        txn->set_lock_set(lock_data_id);
        return true;
    }
}

/**
 * @description: 申请表级写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_exclusive_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    txn->set_state(TransactionState::GROWING);
    while (true) {
        LockDataId lock_data_id = {tab_fd, LockDataType::TABLE};
        auto &request_queue = lock_table_[lock_data_id];
        int flag = 0;
        for (auto &request: request_queue.request_queue_) {
            if (request.txn_id_ != txn->get_transaction_id()) {
                if (request.lock_mode_ == LockMode::EXLUCSIVE) flag = 1;
                if (request.lock_mode_ == LockMode::INTENTION_EXCLUSIVE) flag = 1;
                if (request.lock_mode_ == LockMode::SHARED) flag = 1;
                if (request.lock_mode_ == LockMode::INTENTION_SHARED) flag = 1;
                if (request.lock_mode_ == LockMode::S_IX) flag = 1;
            }
        }
        if (flag == 1) {
            lock.unlock();
            sleep(1);
            lock.lock();
            continue;
        }
        // 表上加X锁
        LockRequest lock_request = {txn->get_transaction_id(), LockMode::EXLUCSIVE};
        request_queue.group_lock_mode_ = GroupLockMode::X;
        for (auto &request : request_queue.request_queue_) {
            if (request.txn_id_ == txn->get_transaction_id()) {
                request.lock_mode_ = LockMode::EXLUCSIVE;
                return true;
            }
        }
        request_queue.request_queue_.push_back(lock_request);
        txn->set_lock_set(lock_data_id);
        return true;
    }
}

/**
 * @description: 申请表级意向读锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IS_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    txn->set_state(TransactionState::GROWING);
    while (true) {
        LockDataId lock_data_id = {tab_fd, LockDataType::TABLE};
        auto &lock_request_queue = lock_table_[lock_data_id];

        // 表上有X锁不能申请
        if (lock_request_queue.group_lock_mode_ == GroupLockMode::X) {
            lock.unlock();
            sleep(1);
            lock.lock();
            continue;
        }

        if (lock_request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
            lock_request_queue.group_lock_mode_ = GroupLockMode::IS;
        }
        LockRequest lock_request = {txn->get_transaction_id(), LockMode::INTENTION_SHARED};
        for (auto &request : lock_request_queue.request_queue_) {
            if (request.txn_id_ == txn->get_transaction_id()) {
                return true;
            }
        }
        lock_request_queue.request_queue_.push_back(lock_request);
        txn->set_lock_set(lock_data_id);
        return true;
    }
}

/**
 * @description: 申请表级意向写锁
 * @return {bool} 返回加锁是否成功
 * @param {Transaction*} txn 要申请锁的事务对象指针
 * @param {int} tab_fd 目标表的fd
 */
bool LockManager::lock_IX_on_table(Transaction* txn, int tab_fd) {
    std::unique_lock<std::mutex> lock(latch_);
    txn->set_state(TransactionState::GROWING);
    while (true) {
        LockDataId lock_data_id = {tab_fd, LockDataType::TABLE};
        auto &lock_request_queue = lock_table_[lock_data_id];

        // 表上有X锁不能申请
        if (lock_request_queue.group_lock_mode_ == GroupLockMode::X) {
            lock.unlock();
            sleep(1);
            lock.lock();
            continue;
        }

        // 表上有S锁不能申请
        if (lock_request_queue.group_lock_mode_ == GroupLockMode::S) {
            lock.unlock();
            sleep(1);
            lock.lock();
            continue;
        }

        lock_request_queue.group_lock_mode_ = GroupLockMode::IX;
        auto &request_queue = lock_table_[lock_data_id];
        LockRequest lock_request = {txn->get_transaction_id(), LockMode::INTENTION_EXCLUSIVE};
        for (auto &request : lock_request_queue.request_queue_) {
            if (request.txn_id_ == txn->get_transaction_id()) {
                request.lock_mode_ = LockMode::INTENTION_EXCLUSIVE;
                return true;
            }
        }
        request_queue.request_queue_.push_back(lock_request);
        txn->set_lock_set(lock_data_id);
        return true;
    }
}

/**
 * @description: 释放锁
 * @return {bool} 返回解锁是否成功
 * @param {Transaction*} txn 要释放锁的事务对象指针
 * @param {LockDataId} lock_data_id 要释放的锁ID
 */
bool LockManager::unlock(Transaction* txn, LockDataId lock_data_id) {
    std::unique_lock<std::mutex> lock(latch_);
    txn->set_state(TransactionState::SHRINKING);
    auto &request_queue = lock_table_[lock_data_id];
    auto lock_request = request_queue.request_queue_.begin();
    auto now = request_queue.request_queue_.end();
    for (; lock_request != request_queue.request_queue_.end(); ++lock_request) {
        if (lock_request->txn_id_ == txn->get_transaction_id()) {
            now = lock_request;
//            break;
        }
    }
    if (now == request_queue.request_queue_.end()) return false;
    request_queue.request_queue_.erase(now);
    request_queue.group_lock_mode_ = GroupLockMode::NON_LOCK;
    for (lock_request = request_queue.request_queue_.begin(); lock_request != request_queue.request_queue_.end(); ++lock_request) {
        if (request_queue.group_lock_mode_ == GroupLockMode::NON_LOCK) {
            if (lock_request->lock_mode_ == LockMode::SHARED) {
                request_queue.group_lock_mode_ = GroupLockMode::S;
                break;
            }
            else if (lock_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) request_queue.group_lock_mode_ = GroupLockMode::IX;
            else if (lock_request->lock_mode_ == LockMode::INTENTION_SHARED) request_queue.group_lock_mode_ = GroupLockMode::IS;
            else if (lock_request->lock_mode_ == LockMode::EXLUCSIVE) {
                request_queue.group_lock_mode_ = GroupLockMode::X;
                break;
            }
            else if (lock_request->lock_mode_ == LockMode::S_IX) {
                request_queue.group_lock_mode_ = GroupLockMode::SIX;
                break;
            }
        } else if (request_queue.group_lock_mode_ == GroupLockMode::IS) {
            if (lock_request->lock_mode_ == LockMode::INTENTION_EXCLUSIVE) {
                request_queue.group_lock_mode_ = GroupLockMode::IX;
                break;
            }
        }
    }
    return true;
}