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

class NestedLoopJoinExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> left_;    // 左儿子节点（需要join的表）
    std::unique_ptr<AbstractExecutor> right_;   // 右儿子节点（需要join的表）
    size_t len_;                                // join后获得的每条记录的长度
    std::vector<ColMeta> cols_;                 // join后获得的记录的字段

    std::vector<Condition> fed_conds_;          // join条件
    std::vector<std::unique_ptr<RmRecord>> left_v;
    int head;
    bool isend;

public:
    NestedLoopJoinExecutor(std::unique_ptr<AbstractExecutor> left, std::unique_ptr<AbstractExecutor> right,
                           std::vector<Condition> conds) {
        left_ = std::move(left);
        right_ = std::move(right);
        len_ = left_->tupleLen() + right_->tupleLen();
        cols_ = left_->cols();
        auto right_cols = right_->cols();
        for (auto &col: right_cols) {
            col.offset += left_->tupleLen();
        }

        cols_.insert(cols_.end(), right_cols.begin(), right_cols.end());
        isend = false;
        fed_conds_ = std::move(conds);
    }

    std::string getType() override { return "NestedLoopJoinExecutor"; };

    size_t tupleLen() const override { return len_; };

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    virtual bool is_end() const { return isend; };

    void beginTuple() override {
        //以左节点为外表，右节点作为内表作笛卡尔积
        left_->beginTuple();
        right_->beginTuple();
        if(left_->is_end() || right_->is_end()){
            isend = true;
            return;
        }
        head = 0;
        find_rec();
    }

    void nextTuple() override {
        // 先向下走一步
        if(is_end()) return;
        head++;
        find_rec();
    }

    void find_rec(){
        while (!left_->is_end() && left_v.size() < 100) {
            left_v.push_back(left_->Next());
            left_->nextTuple();
        }
        while(!right_->is_end()){
            auto rec = std::make_unique<RmRecord>(len_);
            //获取左节点和右节点的记录，并拼接
            auto rec_r = right_->Next();
            for (;head < left_v.size(); ++head) {
                memcpy(rec->data, left_v[head]->data, left_->tupleLen());
                memcpy(rec->data + left_->tupleLen(), rec_r->data, right_->tupleLen());
                //判断是否符合条件
                if(fed_conds_.empty() || eval_conds(cols_, fed_conds_, rec.get())) {
                    //找到符合条件即return
                    return;
                }
            }

            //若不符合则继续找
            right_->nextTuple();
            head = 0;
            if(right_->is_end()){
                left_v.clear();
                if (left_->is_end()) {
                    isend = true;
                    return;
                }
                while (!left_->is_end() && left_v.size() < 100) {
                    left_v.push_back(left_->Next());
                    left_->nextTuple();
                }
                right_->beginTuple();
            }
        }
        //若到最后都没找到则说明没有记录了
        isend = true;
    }

    std::unique_ptr<RmRecord> Next() override {
        auto rec = std::make_unique<RmRecord>(len_);
        //获取左节点和右节点的记录，并拼接
        auto rec_r = right_->Next();
        memcpy(rec->data, left_v[head]->data, left_->tupleLen());
        memcpy(rec->data + left_->tupleLen(), rec_r->data, right_->tupleLen());
        return rec;
    }

    Rid &rid() override { return _abstract_rid; }
};