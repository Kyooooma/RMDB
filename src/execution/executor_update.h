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

#include <utility>

#include "execution_defs.h"
#include "execution_manager.h"
#include "executor_abstract.h"
#include "index/ix.h"
#include "system/sm.h"

class UpdateExecutor : public AbstractExecutor {
private:
    TabMeta tab_;
    size_t len_;
    std::vector<Condition> conds_;
    RmFileHandle *fh_;
    std::vector<Rid> rids_;
    std::string tab_name_;
    std::vector<SetClause> set_clauses_;
    SmManager *sm_manager_;

public:
    UpdateExecutor(SmManager *sm_manager, const std::string &tab_name, std::vector<SetClause> set_clauses,
                   std::vector<Condition> conds, std::vector<Rid> rids, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = tab_name;
        set_clauses_ = std::move(set_clauses);
        tab_ = sm_manager_->db_.get_table(tab_name);
        fh_ = sm_manager_->fhs_.at(tab_name).get();
        conds_ = std::move(conds);
        rids_ = std::move(rids);
        context_ = context;
        for(const auto& i : tab_.cols){
            len_ += i.len;
        }
    }

    std::string getType() override { return "UpdateExecutor"; };

    size_t tupleLen() const override { return len_; };

    void delete_index(RmRecord* rec){
        // 删除索引
        for (auto &index: tab_.indexes) {
            auto ih = sm_manager_->ihs_.at(sm_manager_->get_ix_manager()->get_index_name(tab_name_, index.cols)).get();
            char *key = new char[index.col_tot_len];
            int offset = 0;
            for (size_t j = 0; j < index.col_num; ++j) {
                memcpy(key + offset, rec->data + index.cols[j].offset, index.cols[j].len);
                offset += index.cols[j].len;
            }
            ih->delete_entry(key, context_->txn_);
            free(key);
        }
    }

    bool insert_index(RmRecord* rec, Rid rid_){
        // 插入索引
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
            auto result = ih->insert_entry(key, rid_, context_->txn_);
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
                ih->delete_entry(key, context_->txn_);
                free(key);
            }
            return false;
        }
        return true;
    }

    std::unique_ptr<RmRecord> Next() override {
        //构建mp
        std::map<TabCol, ColMeta> mp;
        for (const auto &i: set_clauses_) {
            ColMeta col = *get_col(tab_.cols, i.lhs);
            mp[i.lhs] = col;
        }
        bool is_fail = false;
        int upd_cnt = 0;
        for (auto rid: rids_) {
            //查找记录
            auto rec = fh_->get_record(rid, context_);
//            std::unique_ptr<RmRecord> rec = fh_->get_record(rid, context_);
            delete_index(rec.get());
            //更新事务
            auto *wr = new WriteRecord(WType::UPDATE_TUPLE, tab_name_, rid, *rec);
            context_->txn_->append_write_record(wr);
            upd_cnt++;
            for (const auto &i: set_clauses_) {
                auto col = mp[i.lhs];
                auto value = i.rhs;
                if(value.type != col.type){
                    Value b = {.type = col.type};
                    convert(value, b);
                    if(value.type != col.type){
                        throw IncompatibleTypeError(coltype2str(col.type), coltype2str(value.type));
                    }
                }
                value.init_raw(col.len);
                //更新记录数据
                memcpy(rec->data + col.offset, value.raw->data, col.len);
            }
            if(!insert_index(rec.get(), rid)){
                is_fail = true;
                auto last = context_->txn_->get_last_write_record();
                auto type = last->GetWriteType();
                assert(type == WType::UPDATE_TUPLE);
                auto rid_ = last->GetRid();
                auto tab_name = last->GetTableName();
                auto rec_ = last->GetRecord();
                insert_index(&rec_, rid_);
                upd_cnt--;
                context_->txn_->delete_write_record();
                break;
            }
            //更新记录
            fh_->update_record(rid, rec->data, context_);
        }

        if(is_fail){
            //插入失败
            while(upd_cnt--){
                auto last = context_->txn_->get_last_write_record();
                auto type = last->GetWriteType();
                assert(type == WType::UPDATE_TUPLE);
                auto rid_ = last->GetRid();
                auto tab_name = last->GetTableName();
                auto rec_ = last->GetRecord();
                auto now_rec = fh_->get_record(rid_, context_);
                delete_index(now_rec.get());
                insert_index(&rec_, rid_);
                fh_->update_record(rid_, rec_.data, context_);
                context_->txn_->delete_write_record();
            }
            throw RMDBError("update error!!");
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};