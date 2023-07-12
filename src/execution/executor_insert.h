/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class InsertExecutor : public AbstractExecutor {
private:
    TabMeta tab_;                   // 表的元数据
    std::vector<Value> values_;     // 需要插入的数据
    RmFileHandle *fh_;              // 表的数据文件句柄
    std::string tab_name_;          // 表名称
    Rid rid_{};                       // 插入的位置，由于系统默认插入时不指定位置，因此当前rid_在插入后才赋值
    SmManager *sm_manager_;

public:
    InsertExecutor(SmManager *sm_manager, const std::string &tab_name, const std::vector<Value>& values, Context *context) {
        sm_manager_ = sm_manager;
        tab_ = sm_manager_->db_.get_table(tab_name);
        values_ = values;
        tab_name_ = tab_name;
        if (values.size() != tab_.cols.size()) {
            throw InvalidValueCountError();
        }
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        context_ = context;
        context_->lock_mgr_->lock_exclusive_on_table(context->txn_, sm_manager_->fhs_[tab_name_]->GetFd());
    };

    std::string getType() override { return "InsertExecutor"; };

    std::unique_ptr<RmRecord> Next() override {
        int fail_pos = -1;
        RmRecord rec(fh_->get_file_hdr().record_size);
        //获取数据
        for (size_t i = 0; i < values_.size(); i++) {
            auto &col = tab_.cols[i];
            auto &val = values_[i];
            if(val.type != col.type){
                Value b = {.type = col.type};
                convert(val, b);
                if(val.type != col.type){
                    throw IncompatibleTypeError(coltype2str(col.type), coltype2str(val.type));
                }
            }
            val.init_raw(col.len);
            memcpy(rec.data + col.offset, val.raw->data, col.len);
        }

        // 插入记录, 获取rid
        //实际插入
        rid_ = fh_->insert_record(rec.data, context_);
        //更新日志-插入
        auto *logRecord = new InsertLogRecord(context_->txn_->get_transaction_id(), rec, rid_,tab_name_);
        logRecord->prev_lsn_ = context_->txn_->get_prev_lsn();
        context_->log_mgr_->add_log_to_buffer(logRecord);
        context_->txn_->set_prev_lsn(logRecord->lsn_);
        // 更新索引
        for (int i = 0; i < tab_.indexes.size(); i++) {
            auto &index = tab_.indexes[i];
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            char *key = new char[index.col_tot_len];
            int offset = 0;
            for (size_t j = 0; j < index.col_num; ++j) {
                memcpy(key + offset, rec.data + index.cols[j].offset, index.cols[j].len);
                offset += index.cols[j].len;
            }
            auto result = ih->insert_entry(key, rid_, context_->txn_);
            free(key);
            if(!result.second){
                //说明插入失败
                fail_pos = i;
                break;
            }
        }

        if(fail_pos != -1){
            //说明插入失败, 将之前插入的索引删除
            for (int i = 0; i < fail_pos; i++) {
                auto index = tab_.indexes[i];
                auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
                char *key = new char[index.col_tot_len];
                int offset = 0;
                for (size_t j = 0; j < index.col_num; ++j) {
                    memcpy(key + offset, rec.data + index.cols[j].offset, index.cols[j].len);
                    offset += index.cols[j].len;
                }
                ih->delete_entry(key, context_->txn_);
                free(key);
            }
            //更新日志
            auto *logRecord_ = new DeleteLogRecord(context_->txn_->get_transaction_id(), rec, rid_,tab_name_);
            logRecord_->prev_lsn_ = context_->txn_->get_prev_lsn();
            context_->log_mgr_->add_log_to_buffer(logRecord_);
            context_->txn_->set_prev_lsn(logRecord_->lsn_);
            //实际删除
            fh_->delete_record(rid_, context_);
            throw RMDBError("Insert Error!!");
        }

        //更新事务
        auto *wr = new WriteRecord(WType::INSERT_TUPLE, tab_name_, rid_, rec);
        context_->txn_->append_write_record(wr);
        return nullptr;
    }

    Rid &rid() override { return rid_; }
};