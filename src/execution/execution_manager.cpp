/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "execution_manager.h"

#include "executor_delete.h"
#include "executor_index_scan.h"
#include "executor_insert.h"
#include "executor_nestedloop_join.h"
#include "executor_projection.h"
#include "executor_seq_scan.h"
#include "executor_update.h"
#include "index/ix.h"
#include "record_printer.h"

const char *help_info = "Supported SQL syntax:\n"
                        "  command ;\n"
                        "command:\n"
                        "  CREATE TABLE table_name (column_name type [, column_name type ...])\n"
                        "  DROP TABLE table_name\n"
                        "  CREATE INDEX table_name (column_name)\n"
                        "  DROP INDEX table_name (column_name)\n"
                        "  INSERT INTO table_name VALUES (value [, value ...])\n"
                        "  DELETE FROM table_name [WHERE where_clause]\n"
                        "  UPDATE table_name SET column_name = value [, column_name = value ...] [WHERE where_clause]\n"
                        "  SELECT selector FROM table_name [WHERE where_clause]\n"
                        "type:\n"
                        "  {INT | FLOAT | CHAR(n) | BIGINT | DATETIME}\n"
                        "where_clause:\n"
                        "  condition [AND condition ...]\n"
                        "condition:\n"
                        "  column op {column | value}\n"
                        "column:\n"
                        "  [table_name.]column_name\n"
                        "op:\n"
                        "  {= | <> | < | > | <= | >=}\n"
                        "selector:\n"
                        "  {* | column [, column ...]}\n";

// 主要负责执行DDL语句
void QlManager::run_mutli_query(std::shared_ptr<Plan> plan, Context *context) {
    if (auto x = std::dynamic_pointer_cast<DDLPlan>(plan)) {
        switch (x->tag) {
            case T_CreateTable: {
                sm_manager_->create_table(x->tab_name_, x->cols_, context);
                break;
            }
            case T_DropTable: {
                sm_manager_->drop_table(x->tab_name_, context);
                break;
            }
            case T_ShowIndex: {
                sm_manager_->show_index(x->tab_name_, context);
                break;
            }
            case T_CreateIndex: {
                sm_manager_->create_index(x->tab_name_, x->tab_col_names_, context);
                break;
            }
            case T_DropIndex: {
                sm_manager_->drop_index(x->tab_name_, x->tab_col_names_, context);
                break;
            }
            default:
                throw InternalError("Unexpected field type");
                break;
        }
    } else if (auto x = std::dynamic_pointer_cast<LOADPlan>(plan)) {
        sm_manager_->load_record(x->file_name_, x->tab_name_, context);
    }
}

// 执行help; show tables; desc table; begin; commit; abort;语句
void QlManager::run_cmd_utility(std::shared_ptr<Plan> plan, txn_id_t *txn_id, Context *context) {
    if (auto x = std::dynamic_pointer_cast<OtherPlan>(plan)) {
        switch (x->tag) {
            case T_Help: {
                memcpy(context->data_send_ + *(context->offset_), help_info, strlen(help_info));
                *(context->offset_) = strlen(help_info);
                break;
            }
            case T_ShowTable: {
                sm_manager_->show_tables(context);
                break;
            }
            case T_DescTable: {
                sm_manager_->desc_table(x->tab_name_, context);
                break;
            }
            case T_Transaction_begin: {
                // 显示开启一个事务
                context->txn_->set_txn_mode(true);
                break;
            }
            case T_Transaction_commit: {
                txn_mgr_->commit(context->txn_, context->log_mgr_);
                break;
            }
            case T_Transaction_rollback: {
                txn_mgr_->abort(context, context->log_mgr_);
                break;
            }
            case T_Transaction_abort: {
                txn_mgr_->abort(context, context->log_mgr_);
                break;
            }
            default:
                throw InternalError("Unexpected field type");
                break;
        }

    }
}

// 执行select语句，select语句的输出除了需要返回客户端外，还需要写入output.txt文件中
void QlManager::select_from(std::unique_ptr<AbstractExecutor> executorTreeRoot, std::vector<TabCol> sel_cols,
                            Context *context) {
    std::vector<std::string> captions;
    captions.reserve(sel_cols.size());
    for (auto &sel_col: sel_cols) {
        if (!sel_col.as_name.empty()) captions.push_back(sel_col.as_name);
        else captions.push_back(sel_col.col_name);
    }

    // Print header into buffer
    RecordPrinter rec_printer(sel_cols.size());
    rec_printer.print_separator(context);
    rec_printer.print_record(captions, context);
    rec_printer.print_separator(context);
    // print header into file
    std::fstream outfile;
    if (!context->output_ellipsis_) {
        outfile.open("output.txt", std::ios::out | std::ios::app);
        outfile << "|";
        for (const auto &caption: captions) {
            outfile << " " << caption << " |";
        }
        outfile << "\n";
    }

    // Print records
    size_t num_rec = 0;
    // 执行query_plan

    // 聚合函数
    if (!sel_cols.begin()->aggregate.empty()) {
        std::string type = sel_cols.begin()->aggregate;
        int ans1 = 0;
        double ans2 = 0;
        std::string ans3;
        int flag = 0;
        num_rec = 1;
        if (type == "count") flag = 1;
        for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
            auto Tuple = executorTreeRoot->Next();
            std::vector<std::string> columns;
            for (auto &col: executorTreeRoot->cols()) {
                char *rec_buf = Tuple->data + col.offset;
                if (type == "count") ans1++, flag = 1;
                if (col.type == TYPE_INT) {
                    if (type == "sum") ans1 += *(int *) rec_buf, flag = 1;
                    if (type == "max") {
                        if (flag == 0) ans1 = *(int *) rec_buf, flag = 1;
                        else ans1 = std::max(ans1, *(int *) rec_buf);
                    }
                    if (type == "min") {
                        if (flag == 0) ans1 = *(int *) rec_buf, flag = 1;
                        else ans1 = std::min(ans1, *(int *) rec_buf);
                    }
                } else if (col.type == TYPE_FLOAT) {
                    if (type == "sum") ans2 += *(double *) rec_buf, flag = 2;
                    if (type == "max") {
                        if (flag == 0) ans2 = *(double *) rec_buf, flag = 2;
                        else ans2 = std::max(ans2, *(double *) rec_buf);
                    }
                    if (type == "min") {
                        if (flag == 0) ans2 = *(double *) rec_buf, flag = 2;
                        else ans2 = std::min(ans2, *(double *) rec_buf);
                    }
                } else if (col.type == TYPE_STRING) {
                    if (type == "max") {
                        if (flag == 0) ans3 = std::string((char *) rec_buf, col.len), flag = 3;
                        else ans3 = std::max(ans3, std::string((char *) rec_buf, col.len));
                    }
                    if (type == "min") {
                        if (flag == 0) ans3 = std::string((char *) rec_buf, col.len), flag = 3;
                        else ans3 = std::min(ans3, std::string((char *) rec_buf, col.len));
                    }
                }
            }
        }
        std::vector<std::string> columns;
        if (!context->output_ellipsis_) {
            outfile << "|";
            if (flag == 1) {
                outfile << " " << std::to_string(ans1) << " |";
            } else if (flag == 2) {
                outfile << " " << std::to_string(ans2) << " |";
            } else {
                outfile << " " << ans3 << " |";
            }
            outfile << "\n";
        }
        if (flag == 1) {
            columns.push_back(std::to_string(ans1));
        } else if (flag == 2) {
            columns.push_back(std::to_string(ans2));
        } else {
            columns.push_back(ans3);
        }
        rec_printer.print_record(columns, context);
    }

        // 正常select
    else {
        for (executorTreeRoot->beginTuple(); !executorTreeRoot->is_end(); executorTreeRoot->nextTuple()) {
            auto Tuple = executorTreeRoot->Next();
            std::vector<std::string> columns;
            for (auto &col: executorTreeRoot->cols()) {
                std::string col_str;
                char *rec_buf = Tuple->data + col.offset;
                if (col.type == TYPE_INT) {
                    col_str = std::to_string(*(int *) rec_buf);
                } else if (col.type == TYPE_FLOAT) {
                    col_str = std::to_string(*(double *) rec_buf);
                } else if (col.type == TYPE_STRING) {
                    col_str = std::string((char *) rec_buf, col.len);
                    col_str.resize(strlen(col_str.c_str()));
                } else if (col.type == TYPE_BIGINT) {
                    col_str = std::to_string(*(long long *) rec_buf);
                } else if (col.type == TYPE_DATETIME) {
                    col_str = AbstractExecutor::datetime2string(*(long long *) rec_buf);
                }
                columns.push_back(col_str);
            }
            // print record into buffer
            rec_printer.print_record(columns, context);
            // print record into file
            if (!context->output_ellipsis_) {
                outfile << "|";
                for (const auto &column: columns) {
                    outfile << " " << column << " |";
                }
                outfile << "\n";
            }
            num_rec++;
        }
    }
    if (!context->output_ellipsis_) {
        outfile.close();
    }
    // Print footer into buffer
    rec_printer.print_separator(context);
    // Print record count into buffer
    RecordPrinter::print_record_count(num_rec, context);
}

// 执行DML语句
void QlManager::run_dml(std::unique_ptr<AbstractExecutor> exec) {
    exec->Next();
}