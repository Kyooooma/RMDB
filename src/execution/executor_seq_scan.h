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

class SeqScanExecutor : public AbstractExecutor {
private:
    std::string tab_name_;              // 表的名称
    std::vector<Condition> conds_;      // scan的条件
    RmFileHandle *fh_;                  // 表的数据文件句柄
    std::vector<ColMeta> cols_;         // scan后生成的记录的字段
    size_t len_;                        // scan后生成的每条记录的长度
    std::vector<Condition> fed_conds_;  // 同conds_，两个字段相同

    Rid rid_;                           // 当前扫描到的记录的rid,Next()返回该rid对应的records
    std::unique_ptr<RecScan> scan_;     // table_iterator

    SmManager *sm_manager_;

public:
    SeqScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds, Context *context) {
        sm_manager_ = sm_manager;
        tab_name_ = std::move(tab_name);
        conds_ = std::move(conds);
        TabMeta &tab = sm_manager_->db_.get_table(tab_name_);
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab.cols;
        len_ = cols_.back().offset + cols_.back().len;

        context_ = context;

        fed_conds_ = conds_;
    }

    /**
     * @brief 构建表迭代器scan_,并开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void beginTuple() override {
        // 构建scan_
        scan_ = std::make_unique<RmScan>(fh_);

        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            try {
                auto rec = fh_->get_record(rid_, context_);
                if (fed_conds_.empty() || eval_conds(cols_, fed_conds_, rec.get())) {
                    break;
                }
            } catch (RecordNotFoundError &e) {
                std::cerr << e.what() << std::endl;
            }
            scan_->next();
        }

    }

    /**
     * @brief 从当前scan_指向的记录开始迭代扫描,直到扫描到第一个满足谓词条件的元组停止,并赋值给rid_
     *
     */
    void nextTuple() override {
        if (!scan_->is_end()) {
            scan_->next();
        }
        while (!scan_->is_end()) {
            rid_ = scan_->rid();
            try {
                auto rec = fh_->get_record(rid_, context_);
                if (fed_conds_.empty() || eval_conds(cols_, fed_conds_, rec.get())) {
                    break;
                }
            } catch (RecordNotFoundError &e) {
                std::cerr << e.what() << std::endl;
            }
            scan_->next();
        }
    }

    /**
     * @brief 返回下一个满足扫描条件的记录
     *
     * @return std::unique_ptr<RmRecord>
     */
    std::unique_ptr<RmRecord> Next() override {
        return fh_->get_record(rid_, context_);
    }

    bool is_end() const override { return scan_->is_end(); }

    Rid &rid() override { return rid_; }

    Value get_Value(ColType p, const char *a) {
        Value res;
        switch (p) {
            case TYPE_INT: {
                int ia = *(int *) a;
                res.set_int(ia);
                break;
            }
            case TYPE_FLOAT: {
                float fa = *(float *) a;
                res.set_float(fa);
                break;
            }
            default:
                throw InternalError("Unexpected data type");
        }
        return res;
    }

    bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const RmRecord *rec) {
        auto lhs_col = get_col(rec_cols, cond.lhs_col);
        char *lhs = rec->data + lhs_col->offset;
        char *rhs;
        ColType rhs_type, lhs_type = lhs_col->type;
        if (cond.is_rhs_val) {
            rhs_type = cond.rhs_val.type;
            rhs = cond.rhs_val.raw->data;
        } else {
            // rhs is a column
            auto rhs_col = get_col(rec_cols, cond.rhs_col);
            rhs_type = rhs_col->type;
            rhs = rec->data + rhs_col->offset;
        }
        int cmp;
        if (rhs_type != lhs_type) {
            Value ls = get_Value(lhs_type, lhs);
            Value rs = get_Value(rhs_type, rhs);
            cmp = dif_compare(ls, rs);
        } else {
            cmp = ix_compare(lhs, rhs, rhs_type, lhs_col->len);
        }
        std::cerr << cmp << '\n';
        if (cond.op == OP_EQ) {
            return cmp == 0;
        } else if (cond.op == OP_NE) {
            return cmp != 0;
        } else if (cond.op == OP_LT) {
            return cmp < 0;
        } else if (cond.op == OP_GT) {
            return cmp > 0;
        } else if (cond.op == OP_LE) {
            return cmp <= 0;
        } else if (cond.op == OP_GE) {
            return cmp >= 0;
        } else {
            throw InternalError("Unexpected op type");
        }
    }

    bool eval_conds(const std::vector<ColMeta> &rec_cols, const std::vector<Condition> &conds, const RmRecord *rec) {
        return std::all_of(conds.begin(), conds.end(),
                           [&](const Condition &cond) { return eval_cond(rec_cols, cond, rec); });
    }

    virtual const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    static void convert(Value &a, Value &b) {
        if (a.type == TYPE_FLOAT) {
            if (b.type == TYPE_INT) {
                b.set_float((float) b.int_val);
                return;
            }
            throw InternalError("convert::Unexpected op type");
        } else if (a.type == TYPE_INT) {
            if (b.type == TYPE_FLOAT) {
                a.set_float((float) a.int_val);
                return;
            }
            throw InternalError("convert::Unexpected op type");
        }
    }

    static inline int dif_compare(Value &pa, Value &pb) {
        convert(pa, pb);
        switch (pa.type) {
            case TYPE_FLOAT:{
                float va = pa.float_val;
                float vb = pb.float_val;
                return (va < vb) ? -1 : ((va > vb) ? 1 : 0);
            }
            case TYPE_INT: {
                int va = pa.int_val;
                int vb = pb.int_val;
                return (va < vb) ? -1 : ((va > vb) ? 1 : 0);
            }
            default:
                throw InternalError("Unexpected data type");
        }
    }
};