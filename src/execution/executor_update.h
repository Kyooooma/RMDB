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
    }

    std::string getType() override { return "UpdateExecutor"; };

    std::unique_ptr<RmRecord> Next() override {
        //构建mp
        std::map<TabCol, ColMeta> mp;
        for (const auto &i: set_clauses_) {
            ColMeta col = *get_col(tab_.cols, i.lhs);
            mp[i.lhs] = col;
        }
        for (auto rid: rids_) {
            //查找记录
            std::unique_ptr<RmRecord> rec = fh_->get_record(rid, context_);
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
            //更新记录
            fh_->update_record(rid, rec->data, context_);
        }
        return nullptr;
    }

    Rid &rid() override { return _abstract_rid; }
};