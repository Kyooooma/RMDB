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

class IndexScanExecutor : public AbstractExecutor {
private:
    std::string tab_name_;                      // 表名称
    TabMeta tab_;                               // 表的元数据
    std::vector<Condition> conds_;              // 扫描条件
    RmFileHandle *fh_;                          // 表的数据文件句柄
    std::vector<ColMeta> cols_;                 // 需要读取的字段
    size_t len_;                                // 选取出来的一条记录的长度
    std::vector<Condition> fed_conds_;          // 扫描条件，和conds_字段相同

    std::vector<std::string> index_col_names_;  // index scan涉及到的索引包含的字段
    IndexMeta index_meta_;                      // index scan涉及到的索引元数据

    Rid rid_{};
    std::unique_ptr<IxScan> scan_;
    IxIndexHandle *ih;
    IxManager *im;
    int index_cnt{};                                    // 匹配的索引字段长度
    SmManager *sm_manager_;

public:
    IndexScanExecutor(SmManager *sm_manager, std::string tab_name, std::vector<Condition> conds,
                      std::vector<std::string> index_col_names,
                      Context *context) {
        sm_manager_ = sm_manager;
        context_ = context;
        tab_name_ = std::move(tab_name);
        tab_ = sm_manager_->db_.get_table(tab_name_);
        conds_ = std::move(conds);
        // index_no_ = index_no;
        im = sm_manager->get_ix_manager();
        std::string ix_name = im->get_index_name(tab_name_, index_col_names);
        if (!sm_manager->ihs_.count(ix_name)) {
            //如果没有打开则打开文件
            sm_manager->ihs_.emplace(ix_name, im->open_index(tab_name_, index_col_names));
        }
        ih = sm_manager->ihs_[ix_name].get();
        index_col_names_ = std::move(index_col_names);
        index_meta_ = *(tab_.get_index_meta(index_col_names_));
        fh_ = sm_manager_->fhs_.at(tab_name_).get();
        cols_ = tab_.cols;
        len_ = cols_.back().offset + cols_.back().len;
        std::map<CompOp, CompOp> swap_op = {
                {OP_EQ, OP_EQ},
                {OP_NE, OP_NE},
                {OP_LT, OP_GT},
                {OP_GT, OP_LT},
                {OP_LE, OP_GE},
                {OP_GE, OP_LE},
        };

        for (auto &cond: conds_) {
            if (cond.lhs_col.tab_name != tab_name_) {
                // lhs is on other table, now rhs must be on this table
                assert(!cond.is_rhs_val && cond.rhs_col.tab_name == tab_name_);
                // swap lhs and rhs
                std::swap(cond.lhs_col, cond.rhs_col);
                cond.op = swap_op.at(cond.op);
            }
        }
        fed_conds_ = conds_;
    }

    std::string getType() override { return "IndexScanExecutor"; };

    size_t tupleLen() const override { return len_; };

    void beginTuple() override {
        std::string ix_name = sm_manager_->get_ix_manager()->get_index_name(tab_name_, index_col_names_);
        std::cout << "index_scan::" << ix_name << "\n\n";
        char *key = new char[index_meta_.col_tot_len];
        Value min_int, min_float, min_datetime;
        {
            min_int.set_int(INT32_MIN);
            min_int.init_raw(sizeof(int));
            min_float.set_float(-1e40);
            min_float.init_raw(sizeof(double));
            min_datetime.set_datetime(10000101000000);
            min_datetime.init_raw(sizeof(long long));
        }
        Value max_int, max_float, max_datetime;
        {
            max_int.set_int(INT32_MAX);
            max_int.init_raw(sizeof(int));
            max_float.set_float(1e40);
            max_float.init_raw(sizeof(double));
            max_datetime.set_datetime(99991231235959);
            max_datetime.init_raw(sizeof(long long));
        }
        int offset = 0, i, f = 1;
        for (i = 0; i < conds_.size() && f; i++) {
            auto cond = conds_[i];
            if (!cond.is_rhs_val || i >= index_col_names_.size() || cond.lhs_col.tab_name != tab_name_ ||
                    cond.lhs_col.col_name != index_col_names_[i] || cond.op == OP_NE)
                break;
            if(cond.op == OP_GE || cond.op == OP_GT){// >= | >
                memcpy(key + offset, cond.rhs_val.raw->data, index_meta_.cols[i].len);
                offset += index_meta_.cols[i].len;
                f = 0;
            }else if(cond.op == OP_LE || cond.op == OP_LT) {// <= | <
                switch (cond.rhs_val.type) {
                    case TYPE_INT: {
                        memcpy(key + offset, min_int.raw->data, index_meta_.cols[i].len);
                        break;
                    }
                    case TYPE_FLOAT:{
                        memcpy(key + offset, min_float.raw->data, index_meta_.cols[i].len);
                        break;
                    }
                    case TYPE_STRING:{
                        Value min_char;
                        std::string val;
                        min_char.set_str(val);
                        min_char.init_raw(index_meta_.cols[i].len);
                        memcpy(key + offset, min_char.raw->data, index_meta_.cols[i].len);
                        break;
                    }
                    case TYPE_DATETIME:{
                        memcpy(key + offset, min_datetime.raw->data, index_meta_.cols[i].len);
                        break;
                    }
                    default:
                        break;
                }
                offset += index_meta_.cols[i].len;
                f = 0;
            }else{// =
                memcpy(key + offset, cond.rhs_val.raw->data, index_meta_.cols[i].len);
                offset += index_meta_.cols[i].len;
            }
        }
        index_cnt = i;
        auto &type = conds_[index_cnt - 1].op;
        int flag = type == OP_GT ? 1 : 0;
        for(; i < index_meta_.cols.size(); i++){
            auto &col = index_meta_.cols[i];
            switch (col.type) {
                case TYPE_INT: {
                    if(flag){
                        memcpy(key + offset, max_int.raw->data, col.len);
                    }else{
                        memcpy(key + offset, min_int.raw->data, col.len);
                    }
                    break;
                }
                case TYPE_FLOAT:{
                    if(flag){
                        memcpy(key + offset, max_float.raw->data, col.len);
                    }else{
                        memcpy(key + offset, min_float.raw->data, col.len);
                    }
                    break;
                }
                case TYPE_STRING:{
                    if(flag){
                        Value str_val;
                        std::string val(col.len, (char)(127));
                        str_val.set_str(val);
                        str_val.init_raw(col.len);
                        memcpy(key + offset, str_val.raw->data, col.len);
                    }else {
                        Value min_char;
                        std::string val;
                        min_char.set_str(val);
                        min_char.init_raw(index_meta_.cols[i].len);
                        memcpy(key + offset, min_char.raw->data, col.len);
                    }
                    break;
                }
                case TYPE_DATETIME:{
                    if(flag){
                        memcpy(key + offset, max_datetime.raw->data, index_meta_.cols[i].len);
                    }else{
                        memcpy(key + offset, min_datetime.raw->data, index_meta_.cols[i].len);
                    }
                    break;
                }
                default:
                    break;
            }
            offset += col.len;
        }
        std::cout << index_cnt << '\n';
        Iid start = ih->leaf_begin();
        if(flag) start = ih->upper_bound(key);
        else start = ih->lower_bound(key);
        std::cout << start.page_no << " " << start.slot_no << "\n";
        Iid end = ih->leaf_end();
        scan_ = std::make_unique<IxScan>(ih, start, end, sm_manager_->get_bpm());
        while(!is_end()){
            rid_ = scan_->rid();
            auto rec = fh_->get_record(rid_, context_);
//                auto rec = fh_->get_record(rid_, context_);
            if (fed_conds_.empty() || eval_conds(cols_, fed_conds_, rec.get())) {
                break;
            }
            scan_->next();
        }
        delete[] key;
    }

    void nextTuple() override {
        if (!is_end()) {
            scan_->next();
        }
        while (!is_end()) {
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

    std::unique_ptr<RmRecord> Next() override {
        auto rec = fh_->get_record(rid_, context_);
        return rec;
    }

    const std::vector<ColMeta> &cols() const override {
        return cols_;
    }

    bool is_end() const override{
        if(scan_->is_end()) return true;
        auto rid = scan_->rid();
        auto rec = fh_->get_record(rid, context_);
        for(int i = 0; i < index_cnt; i++){
            if(!eval_cond(cols_, conds_[i], rec.get())) return true;
        }
        return false;
    }

    Rid &rid() override { return rid_; }
};