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

class SortExecutor : public AbstractExecutor {
   private:
    std::unique_ptr<AbstractExecutor> prev_;
    std::vector<ColMeta> cols_;                              // 框架中只支持一个键排序，需要自行修改数据结构支持多个键排序
    size_t tuple_num;
    std::vector<bool> is_desc_;
    std::vector<size_t> used_tuple;
    std::unique_ptr<RmRecord> current_tuple;

   public:
    SortExecutor(std::unique_ptr<AbstractExecutor> prev, const std::vector<TabCol>& sel_cols, std::vector<bool> is_desc) {
        prev_ = std::move(prev);
        for (const auto& sel_col : sel_cols) {
            cols_.push_back(*prev_->get_col(prev_->cols(), sel_col));
        }
        is_desc_ = std::move(is_desc);
        tuple_num = 0;
        used_tuple.clear();
        current_tuple = nullptr;
    }

    std::string getType() override { return "SortExecutor"; };

    const std::vector<ColMeta> &cols() const override {
        return prev_->cols();
    }
    void beginTuple() override {
        prev_->beginTuple();
        int cnt = 0;
        int now = -1;
        current_tuple = nullptr;
        while (!prev_->is_end()) {;
            if (cmp(prev_->Next(), current_tuple)) {
                current_tuple = prev_->Next();
                now = cnt;
            }
            prev_->nextTuple();
            cnt++;
        }
        tuple_num++;
        used_tuple.push_back(now);
    }

    void nextTuple() override {
        prev_->beginTuple();
        int cnt = 0;
        int now = -1;
        current_tuple = nullptr;
        while (!prev_->is_end()) {
            if (std::find(used_tuple.begin(), used_tuple.end(),cnt) == used_tuple.end() && cmp(prev_->Next(), current_tuple)) {
                current_tuple = prev_->Next();
                now = cnt;
            }
            prev_->nextTuple();
            cnt++;
        }
        tuple_num++;
        used_tuple.push_back(now);
    }

    std::unique_ptr<RmRecord> Next() override {
        return std::move(current_tuple);
    }

    Rid &rid() override { return _abstract_rid; }
    bool is_end() const {return used_tuple.back() == -1; };
   private:
    bool cmp(std::unique_ptr<RmRecord> a, std::unique_ptr<RmRecord> &b) {
        if (b == nullptr) {
            return true;
        }
        int cnt = 0;
        for (auto &col: cols_) {
            std::string col_str;
            char *rec_buf_a = a->data + col.offset;
            char *rec_buf_b = b->data + col.offset;
            if (col.type == TYPE_INT) {
                int value_a = *(int *) rec_buf_a;
                int value_b = *(int *) rec_buf_b;
                if (value_a == value_b) {
                    cnt++;
                    continue;
                }
                if (is_desc_[cnt]) return value_a > value_b;
                else return value_a < value_b;
            } else if (col.type == TYPE_FLOAT) {
                double value_a = *(double *) rec_buf_a;
                double value_b = *(double *) rec_buf_b;
                if (value_a == value_b) {
                    cnt++;
                    continue;
                }
                if (is_desc_[cnt]) return value_a > value_b;
                else return value_a < value_b;
            } else if (col.type == TYPE_STRING) {
                std::string value_a = std::string((char *) rec_buf_a, col.len);
                std::string value_b = std::string((char *) rec_buf_b, col.len);
                if (value_a == value_b) {
                    cnt++;
                    continue;
                }
                if (is_desc_[cnt]) return value_a > value_b;
                else return value_a < value_b;
            }
        }
        return true;
    }
};