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
#include <vector>
#include <string>
#include <memory>
#include <algorithm>

enum JoinType {
    INNER_JOIN, LEFT_JOIN, RIGHT_JOIN, FULL_JOIN
};
namespace ast {

    enum SvType {
        SV_TYPE_INT, SV_TYPE_FLOAT, SV_TYPE_STRING, SV_TYPE_BIGINT, SV_TYPE_DATETIME
    };

    enum SvCompOp {// =, !=, <, >, <=, >=
        SV_OP_EQ, SV_OP_NE, SV_OP_LT, SV_OP_GT, SV_OP_LE, SV_OP_GE
    };

    enum SvSetOp {// +, -, set
        SV_OP_ADD, SV_OP_SUB, SV_OP_SET
    };

    enum OrderByDir {
        OrderBy_DEFAULT,
        OrderBy_ASC,
        OrderBy_DESC
    };

// Base class for tree nodes
    struct TreeNode {
        virtual ~TreeNode() = default;  // enable polymorphism
    };

    struct Help : public TreeNode {
    };

    struct ShowTables : public TreeNode {
    };

    struct TxnBegin : public TreeNode {
    };

    struct TxnCommit : public TreeNode {
    };

    struct TxnAbort : public TreeNode {
    };

    struct TxnRollback : public TreeNode {
    };

    struct TypeLen : public TreeNode {
        SvType type;
        int len;

        TypeLen(SvType type_, int len_) : type(type_), len(len_) {}
    };

    struct Field : public TreeNode {
    };

    struct ColDef : public Field {
        std::string col_name;
        std::shared_ptr<TypeLen> type_len;

        ColDef(std::string col_name_, std::shared_ptr<TypeLen> type_len_) :
                col_name(std::move(col_name_)), type_len(std::move(type_len_)) {}
    };

    struct CreateTable : public TreeNode {
        std::string tab_name;
        std::vector<std::shared_ptr<Field>> fields;

        CreateTable(std::string tab_name_, std::vector<std::shared_ptr<Field>> fields_) :
                tab_name(std::move(tab_name_)), fields(std::move(fields_)) {}
    };

    struct DropTable : public TreeNode {
        std::string tab_name;

        DropTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
    };

    struct DescTable : public TreeNode {
        std::string tab_name;

        DescTable(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
    };

    struct CreateIndex : public TreeNode {
        std::string tab_name;
        std::vector<std::string> col_names;

        CreateIndex(std::string tab_name_, std::vector<std::string> col_names_) :
                tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
    };

    struct ShowIndex : public TreeNode {
        std::string tab_name;

        ShowIndex(std::string tab_name_) : tab_name(std::move(tab_name_)) {}
    };

    struct LoadRecord : public TreeNode {
        std::string file_name, tab_name;

        LoadRecord(std::string file_name_, const std::string& suffix, std::string tab_name_){
            file_name = std::move(file_name_);
            file_name += '.';
            file_name += suffix;
            tab_name = std::move(tab_name_);
        }
    };

    struct DropIndex : public TreeNode {
        std::string tab_name;
        std::vector<std::string> col_names;

        DropIndex(std::string tab_name_, std::vector<std::string> col_names_) :
                tab_name(std::move(tab_name_)), col_names(std::move(col_names_)) {}
    };

    struct Expr : public TreeNode {
    };

    struct Value : public Expr {
    };

    struct IntLit : public Value {
        int val;

        IntLit(int val_) : val(val_) {}
    };

    struct FloatLit : public Value {
        double val;

        FloatLit(double val_) : val(val_) {}
    };

    struct BigintLit : public Value {
        long long val;

        BigintLit(long long val_) : val(val_) {}
    };

    struct StringLit : public Value {
        std::string val;

        StringLit(std::string val_) : val(std::move(val_)) {}
    };

    struct DatetimeLit : public Value {
        long long val;

        DatetimeLit(long long val_) : val(val_) {}

        std::string to_string() {
            long long x = val;
            std::vector<int> v(5);
            for (int i = 0; i < 5; i++) {
                v[i] = x % 100;
                x /= 100;
            }

            std::reverse(v.begin(), v.end());
            std::string res = std::to_string(x);
            for (int i = 0; i < 2; i++) {
                std::string t = std::to_string(v[i]);
                if (t.size() < 2) t = "0" + t;
                res += "-" + t;
            }
            res += " ";
            for (int i = 2; i < 5; i++) {
                std::string t = std::to_string(v[i]);
                if (t.size() < 2) t = "0" + t;
                res += t;
                if (i + 1 < 5) res += ":";
            }
            return res;
        }
    };

    struct Col : public Expr {
        std::string tab_name;
        std::string col_name;
        std::string as_name;
        std::string aggregate;

        Col(std::string tab_name_, std::string col_name_) :
                tab_name(std::move(tab_name_)), col_name(std::move(col_name_)) {};

        Col(std::string tab_name_, std::string col_name_, std::string as_name_) :
                tab_name(std::move(tab_name_)), col_name(std::move(col_name_)),
                as_name(std::move(as_name_)) {};

        Col(std::string tab_name_, std::string col_name_, std::string as_name_, std::string aggregate_) :
                tab_name(std::move(tab_name_)), col_name(std::move(col_name_)),
                as_name(std::move(as_name_)), aggregate(std::move(aggregate_)) {}
    };

    struct SetClause : public TreeNode {
        std::string col_name;
        std::shared_ptr<Value> val;
        SvSetOp setOp;

        SetClause(std::string col_name_, std::shared_ptr<Value> val_, SvSetOp op_ = SV_OP_SET) :
                col_name(std::move(col_name_)), val(std::move(val_)), setOp(op_){}
    };

    struct BinaryExpr : public TreeNode {
        std::shared_ptr<Col> lhs;
        SvCompOp op;
        std::shared_ptr<Expr> rhs;

        BinaryExpr(std::shared_ptr<Col> lhs_, SvCompOp op_, std::shared_ptr<Expr> rhs_) :
                lhs(std::move(lhs_)), op(op_), rhs(std::move(rhs_)) {}
    };

    struct OrderBy : public TreeNode {
        std::shared_ptr<Col> cols;
        OrderByDir orderby_dir;

        OrderBy(std::shared_ptr<Col> cols_, OrderByDir orderby_dir_) :
                cols(std::move(cols_)), orderby_dir(orderby_dir_) {}
    };

    struct Limit : public TreeNode {
        int start;
        int len;

        Limit(int start_, int len_) :
                start(start_), len(len_) {}
    };

    struct InsertStmt : public TreeNode {
        std::string tab_name;
        std::vector<std::shared_ptr<Value>> vals;

        InsertStmt(std::string tab_name_, std::vector<std::shared_ptr<Value>> vals_) :
                tab_name(std::move(tab_name_)), vals(std::move(vals_)) {}
    };

    struct DeleteStmt : public TreeNode {
        std::string tab_name;
        std::vector<std::shared_ptr<BinaryExpr>> conds;

        DeleteStmt(std::string tab_name_, std::vector<std::shared_ptr<BinaryExpr>> conds_) :
                tab_name(std::move(tab_name_)), conds(std::move(conds_)) {}
    };

    struct UpdateStmt : public TreeNode {
        std::string tab_name;
        std::vector<std::shared_ptr<SetClause>> set_clauses;
        std::vector<std::shared_ptr<BinaryExpr>> conds;

        UpdateStmt(std::string tab_name_,
                   std::vector<std::shared_ptr<SetClause>> set_clauses_,
                   std::vector<std::shared_ptr<BinaryExpr>> conds_) :
                tab_name(std::move(tab_name_)), set_clauses(std::move(set_clauses_)), conds(std::move(conds_)) {}
    };

    struct JoinExpr : public TreeNode {
        std::string left;
        std::string right;
        std::vector<std::shared_ptr<BinaryExpr>> conds;
        JoinType type;

        JoinExpr(std::string left_, std::string right_,
                 std::vector<std::shared_ptr<BinaryExpr>> conds_, JoinType type_) :
                left(std::move(left_)), right(std::move(right_)), conds(std::move(conds_)), type(type_) {}
    };

    struct SelectStmt : public TreeNode {
        std::vector<std::shared_ptr<Col>> cols;
        std::vector<std::string> tabs;
        std::vector<std::shared_ptr<BinaryExpr>> conds;
        std::vector<std::shared_ptr<JoinExpr>> jointree;


        bool has_sort;
        std::vector<std::shared_ptr<OrderBy>> order;

        std::shared_ptr<Limit> limit;


        SelectStmt(std::vector<std::shared_ptr<Col>> cols_,
                   std::vector<std::string> tabs_,
                   std::vector<std::shared_ptr<BinaryExpr>> conds_,
                   std::vector<std::shared_ptr<OrderBy>> order_,
                   std::shared_ptr<Limit> limit_) :
                cols(std::move(cols_)), tabs(std::move(tabs_)), conds(std::move(conds_)),
                order(std::move(order_)), limit(std::move(limit_)) {
            has_sort = !order.empty();
        }
    };

// Semantic value
    struct SemValue {
        int sv_int;
        double sv_float;
        long long sv_bigint;
        std::string sv_str;
        long long sv_datetime;
        OrderByDir sv_orderby_dir;
        std::vector<std::string> sv_strs;

        std::shared_ptr<TreeNode> sv_node;

        SvCompOp sv_comp_op;

        std::shared_ptr<TypeLen> sv_type_len;

        std::shared_ptr<Field> sv_field;
        std::vector<std::shared_ptr<Field>> sv_fields;

        std::shared_ptr<Expr> sv_expr;

        std::shared_ptr<Value> sv_val;
        std::vector<std::shared_ptr<Value>> sv_vals;

        std::shared_ptr<Col> sv_col;
        std::vector<std::shared_ptr<Col>> sv_cols;

        std::shared_ptr<SetClause> sv_set_clause;
        std::vector<std::shared_ptr<SetClause>> sv_set_clauses;

        std::shared_ptr<BinaryExpr> sv_cond;
        std::vector<std::shared_ptr<BinaryExpr>> sv_conds;

        std::shared_ptr<OrderBy> sv_orderby;
        std::vector<std::shared_ptr<OrderBy>> sv_orderbys;

        std::shared_ptr<Limit> sv_limit;
    };

    extern std::shared_ptr<ast::TreeNode> parse_tree;

}

#define YYSTYPE ast::SemValue
