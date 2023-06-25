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
#include "common/common.h"
#include "index/ix.h"
#include "system/sm.h"

class AbstractExecutor {
public:
    Rid _abstract_rid;

    Context *context_;

    virtual ~AbstractExecutor() = default;

    virtual size_t tupleLen() const { return 0; };

    virtual const std::vector<ColMeta> &cols() const {
        std::vector<ColMeta> *_cols = nullptr;
        throw RMDBError("怎么不重写cols的");
        return *_cols;
    };

    virtual std::string getType() { return "AbstractExecutor"; };

    virtual void beginTuple(){};

    virtual void nextTuple(){};

    virtual bool is_end() const { return true; };

    virtual Rid &rid() = 0;

    virtual std::unique_ptr<RmRecord> Next() = 0;

    virtual ColMeta get_col_offset(const TabCol &target) { return ColMeta();};

    static std::vector<ColMeta>::const_iterator get_col(const std::vector<ColMeta> &rec_cols, const TabCol &target) {
        auto pos = std::find_if(rec_cols.begin(), rec_cols.end(), [&](const ColMeta &col) {
            return col.tab_name == target.tab_name && col.name == target.col_name;
        });
        if (pos == rec_cols.end()) {
            throw ColumnNotFoundError(target.tab_name + '.' + target.col_name);
        }
        return pos;
    }

    /**
     * @brief datetime类型转换成string
     * @return string
     */
    static std::string datetime2string(long long x){
        std::vector<int> v(5);
        for(int i = 0; i < 5; i++){
            v[i] = x % 100;
            x /= 100;
        }
        std::reverse(v.begin(), v.end());
        std::string res = std::to_string(x);
        for(int i = 0; i < 2; i++){
            std::string t = std::to_string(v[i]);
            if(t.size() < 2) t = "0" + t;
            res += "-" + t;
        }
        res += " ";
        for(int i = 2; i < 5; i++){
            std::string t = std::to_string(v[i]);
            if(t.size() < 2) t = "0" + t;
            res += t;
            if(i + 1 < 5) res += ":";
        }
        return res;
    }

    /**
     * @brief 类型转换
     * @return Value
     */
    static Value get_Value(ColType p, const char *a) {
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
            case TYPE_BIGINT: {
                long long ba = *(long long *) a;
                res.set_bigint(ba);
                break;
            }
            case TYPE_DATETIME: {
                long long da = *(long long *) a;
                res.set_datetime(da);
                break;
            }
            case TYPE_STRING:
                std::string str = a;
                res.set_str(str);
                break;
        }
        return res;
    }

    /**
     * @brief 判断该记录是否满足where条件
     * @return bool
     */
    static bool eval_cond(const std::vector<ColMeta> &rec_cols, const Condition &cond, const RmRecord *rec) {
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
        Value ls = get_Value(lhs_type, lhs);
        Value rs = get_Value(rhs_type, rhs);
        int cmp = val_compare(ls, rs);
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

    static void convert(Value &a, Value &b) {
        // 数值类型的转化(int, float, bigint)
        // int -> float
        // int -> bigint
        // bigint -> float
        // time -> string
        if(a.type == b.type) return;
        if (a.type == TYPE_FLOAT) {
            if (b.type == TYPE_INT) {
                b.set_float((float) b.int_val);
                return;
            }
            if (b.type == TYPE_BIGINT) {
                b.set_float((float) b.bigint_val);
                return;
            }
        } else if (a.type == TYPE_INT) {
            if (b.type == TYPE_FLOAT) {
                a.set_float((float) a.int_val);
                return;
            }
            if (b.type == TYPE_BIGINT) {
                a.set_bigint((long long) a.int_val);
                return;
            }
        } else if (a.type == TYPE_BIGINT) {
            if (b.type == TYPE_INT) {
                b.set_bigint((long long) b.int_val);
                return;
            }
            if (b.type == TYPE_FLOAT) {
                a.set_float((float) a.int_val);
                return;
            }
        } else if(a.type == TYPE_DATETIME){
            if(b.type == TYPE_STRING){
                a.set_str(datetime2string(a.datetime_val));
                return;
            }
        }else if(a.type == TYPE_STRING){
            if(b.type == TYPE_DATETIME){
                b.set_str(datetime2string(b.datetime_val));
                return;
            }
        }
        throw InternalError("convert::Unexpected value type");
    }

    static inline int val_compare(Value &pa, Value &pb) {
        convert(pa, pb);
        switch (pa.type) {
            case TYPE_FLOAT:{
                double va = pa.float_val;
                double vb = pb.float_val;
                return (va < vb) ? -1 : ((va > vb) ? 1 : 0);
            }
            case TYPE_INT: {
                int va = pa.int_val;
                int vb = pb.int_val;
                return (va < vb) ? -1 : ((va > vb) ? 1 : 0);
            }
            case TYPE_BIGINT: {
                long long va = pa.bigint_val;
                long long vb = pb.bigint_val;
                return (va < vb) ? -1 : ((va > vb) ? 1 : 0);
            }
            case TYPE_DATETIME: {
                long long va = pa.datetime_val;
                long long vb = pb.datetime_val;
                return (va < vb) ? -1 : ((va > vb) ? 1 : 0);
            }
            case TYPE_STRING: {
                std::string va = pa.str_val;
                std::string vb = pb.str_val;
                return (va < vb) ? -1 : ((va > vb) ? 1 : 0);
            }
        }
    }
};