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

class ProjectionExecutor : public AbstractExecutor {
private:
    std::unique_ptr<AbstractExecutor> prev_;        // 投影节点的儿子节点
    std::vector<ColMeta> cols_;                     // 需要投影的字段
    size_t len_;                                    // 字段总长度
    std::vector<size_t> sel_idxs_;                  // 投影字段下标
    std::shared_ptr<ast::Limit> limit;
    int cnt;
public:
    ProjectionExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol> &sel_cols, std::shared_ptr<ast::Limit> &limit_) {
        prev_ = std::move(prev);

        int curr_offset = 0;
        auto &prev_cols = prev_->cols();
        for (auto &sel_col: sel_cols) {
            auto pos = get_col(prev_cols, sel_col);
            sel_idxs_.push_back(pos - prev_cols.begin());
            auto col = *pos;
            col.offset = curr_offset;
            curr_offset += col.len;
            cols_.push_back(col);
        }
        len_ = curr_offset;
        limit = std::move(limit_);
        cnt = 0;
    }

    std::string getType() override { return "ProjectionExecutor"; };

    size_t tupleLen() const override { return len_; };

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    void beginTuple() override {
        prev_->beginTuple();
        for (int i = 0; i < limit->start && !prev_->is_end(); ++i) prev_->nextTuple();
    }

    void nextTuple() override {
        assert(!is_end());
        prev_->nextTuple();
    }

    std::unique_ptr<RmRecord> Next() override {
        assert(!is_end());
        cnt++;
        auto proj_rec = std::make_unique<RmRecord>(len_);
        auto &prev_cols = prev_->cols();// 列数据
        auto prev_rec = prev_->Next();// 具体记录
        for (int i = 0; i < sel_idxs_.size(); i++) {
            auto idx = sel_idxs_[i];// 投影列在子节点的下标
            auto col = cols_[i];// 投影列
            memcpy(proj_rec->data + col.offset, prev_rec->data + prev_cols[idx].offset, col.len);
        }
        return proj_rec;
    }

    bool is_end() const override { return prev_->is_end() || cnt == limit->len; }

    Rid &rid() override { return _abstract_rid; }

};