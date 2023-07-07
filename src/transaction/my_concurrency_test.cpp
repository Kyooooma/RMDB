#include "concurrency/lock_manager.h"
#include "transaction_manager.h"
#include "execution/execution_manager.h"
#include "gtest/gtest.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "optimizer/optimizer.h"

#include "errors.h"
#include "analyze/analyze.h"

#define BUFFER_LENGTH 8192
const std::string TEST_DB_NAME = "ConcurrencyTestDB";

enum class LockMode { SHARED, EXCLUSIVE, INTENTION_SHARED, INTENTION_EXCLUSIVE};
std::string lockModeStr[4] = {"shared", "exclusive", "intention shared", "intention exclusive"};
class LockOperation {
   public:
    LockMode lock_mode_;
    LockDataType data_type_;
    int table_;
    Rid rid_;
};
Rid default_rid{-1, -1};

enum class OperationMode { INSERT, DELETE, SELECT, UPDATE};
std::string operationStr[4] = {"insert", "delete", "select", "update"};

class ConcurrencyTest : public ::testing::Test {
   public:
    std::unique_ptr<DiskManager> disk_manager;
    std::unique_ptr<BufferPoolManager> buffer_pool_manager;
    std::unique_ptr<RmManager> rm_manager;
    std::unique_ptr<IxManager> ix_manager;
    std::unique_ptr<SmManager> sm_manager;
    std::unique_ptr<QlManager> ql_manager;
    std::unique_ptr<LogManager> log_manager;
    std::unique_ptr<LockManager> lock_manager;
    std::unique_ptr<TransactionManager> txn_manager;
    std::unique_ptr<Analyze> analyze;
    std::unique_ptr<Planner> planner;
    std::unique_ptr<Optimizer> optimizer;
    std::unique_ptr<Portal> portal;
    std::mutex mutex;

   public:
    void SetUp() override {
        ::testing::Test::SetUp();
        disk_manager = std::make_unique<DiskManager>();
        buffer_pool_manager = std::make_unique<BufferPoolManager>(BUFFER_POOL_SIZE, disk_manager.get());
        rm_manager = std::make_unique<RmManager>(disk_manager.get(), buffer_pool_manager.get());
        ix_manager = std::make_unique<IxManager>(disk_manager.get(), buffer_pool_manager.get());
        sm_manager = std::make_unique<SmManager>(disk_manager.get(), buffer_pool_manager.get(), rm_manager.get(),
                                                 ix_manager.get());
        lock_manager = std::make_unique<LockManager>();
        txn_manager = std::make_unique<TransactionManager>(lock_manager.get(), sm_manager.get());
        ql_manager = std::make_unique<QlManager>(sm_manager.get(), txn_manager.get());
        log_manager = std::make_unique<LogManager>(disk_manager.get());
        planner = std::make_unique<Planner>(sm_manager.get());
        optimizer = std::make_unique<Optimizer>(sm_manager.get(), planner.get());
        portal = std::make_unique<Portal>(sm_manager.get());
        analyze = std::make_unique<Analyze>(sm_manager.get());
        if(sm_manager->is_dir(TEST_DB_NAME)) {
            sm_manager->drop_db(TEST_DB_NAME);
        }
        sm_manager->create_db(TEST_DB_NAME);
        sm_manager->open_db(TEST_DB_NAME);
    }

    void TearDown() override {
        sm_manager->close_db();
    }

    // 判断当前正在执行的是显式事务还是单条SQL语句的事务，并更新事务ID
    void SetTransaction(txn_id_t *txn_id_, Context *context) {
        context->txn_ = txn_manager->get_transaction(*txn_id_);
        if (context->txn_ == nullptr || context->txn_->get_state() == TransactionState::COMMITTED ||
            context->txn_->get_state() == TransactionState::ABORTED) {
            context->txn_ = txn_manager->begin(nullptr, context->log_mgr_);
            *txn_id_ = context->txn_->get_transaction_id();
            context->txn_->set_txn_mode(false);
        }
    }

    void exec_sql(const std::string &sql, char* result, int *offset, txn_id_t *txn_id) {
        std::unique_lock<std::mutex> lock(mutex);
        YY_BUFFER_STATE yy_buffer = yy_scan_string(sql.c_str());
        assert(yyparse() == 0 && ast::parse_tree != nullptr);
        yy_delete_buffer(yy_buffer);
        lock.unlock();
        memset(result, 0, BUFFER_LENGTH);
        *offset = 0;

        Context *context = new Context(lock_manager.get(), log_manager.get(),
                                       nullptr, result, offset);
        SetTransaction(txn_id, context);
        std::shared_ptr<Query> query = analyze->do_analyze(ast::parse_tree);
        // 优化器
        std::shared_ptr<Plan> plan = optimizer->plan_query(query, context);
        // portal
        std::shared_ptr<PortalStmt> portalStmt = portal->start(plan, context);
        portal->run(portalStmt, ql_manager.get(), txn_id, context);
        portal->drop();
        if (!context->txn_->get_txn_mode()) {
            txn_manager->commit(context->txn_, context->log_mgr_);
        }
    }

    void RunLockOperation(Transaction *txn, const LockOperation &operation) {
        switch (operation.lock_mode_) {
            case LockMode::SHARED: {
                if(operation.data_type_ == LockDataType::RECORD) {
                    lock_manager->lock_shared_on_record(txn, operation.rid_, operation.table_);
                }
                else {
                    lock_manager->lock_shared_on_table(txn, operation.table_);
                }
            } break;
            case LockMode::EXCLUSIVE: {
                if(operation.data_type_ == LockDataType::TABLE) {
                    lock_manager->lock_exclusive_on_record(txn, operation.rid_, operation.table_);
                }
                else {
                    lock_manager->lock_shared_on_table(txn, operation.table_);
                }
            } break;
            case LockMode::INTENTION_SHARED: {
                lock_manager->lock_IS_on_table(txn, operation.table_);
            } break;
            case LockMode::INTENTION_EXCLUSIVE: {
                lock_manager->lock_IX_on_table(txn, operation.table_);
            } break;
            default:
                break;
        }
    }
};

TEST_F(ConcurrencyTest, DirtyReadTest) {
    /**
     * pre: create table t1 (id int, num int);
     * t1: begin;
     * t1: insert into t1 values(1,1);
     * t2: begin;
     * t2: select * from t1;
     * t1: abort;
     * check t2 result : 0 records
     * t2: commit;
     */
     char* res = new char[BUFFER_LENGTH];
     int pre_offset;
     int pre_txn_id = INVALID_TXN_ID;
    exec_sql("create table t1 (id int, num int);", res, &pre_offset, &pre_txn_id);

    std::thread t0([&] {
        char *result = new char[BUFFER_LENGTH];
        txn_id_t txn_id = INVALID_TXN_ID;
        int offset;
        exec_sql("begin;", result, &offset, &txn_id);
        exec_sql("insert into t1 values (1, 1);", result, &offset, &txn_id);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        exec_sql("abort;", result, &offset, &txn_id);
    });

    std::thread t1([&] {
        char *result = new char[BUFFER_LENGTH];
        txn_id_t txn_id = INVALID_TXN_ID;
        int offset;
        // sleep thread1 to make thread0 obtain lock first;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        exec_sql("begin;", result, &offset, &txn_id);
        // this sentence
        exec_sql("select * from t1;", result, &offset, &txn_id);

        // txn2 cannot read the aborted records;
        const char* ans = "+------------------+------------------+\n"
            "|               id |              num |\n"
            "+------------------+------------------+\n"
            "+------------------+------------------+\n"
            "Total record(s): 0\n";

        EXPECT_EQ(*result, *ans);

        exec_sql("commit;", result, &offset, &txn_id);
    });

    t0.join();
    t1.join();
}

TEST_F(ConcurrencyTest, ReadCommitedTest) {
    /**
     * pre: create table t1 (id int, num int);
     * t1: begin;
     * t1: insert into t1 values(1,1);
     * t2: begin;
     * t2: select * from t1;
     * t1: commit;
     * check t2 result : 1 records
     * t2: commit;
     */
    char* res = new char[BUFFER_LENGTH];
    int pre_offset;
    int pre_txn_id = INVALID_TXN_ID;
    exec_sql("create table t1 (id int, num int);", res, &pre_offset, &pre_txn_id);

    std::thread t0([&] {
        char *result = new char[BUFFER_LENGTH];
        txn_id_t txn_id = INVALID_TXN_ID;
        int offset;
        exec_sql("begin;", result, &offset, &txn_id);
        exec_sql("insert into t1 values (1, 1);", result, &offset, &txn_id);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        exec_sql("commit;", result, &offset, &txn_id);
    });

    std::thread t1([&] {
        char *result = new char[BUFFER_LENGTH];
        txn_id_t txn_id = INVALID_TXN_ID;
        int offset;
        // sleep thread1 to make thread0 obtain lock first;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        exec_sql("begin;", result, &offset, &txn_id);
        // this sentence
        exec_sql("select * from t1;", result, &offset, &txn_id);

        // txn2 cannot read the aborted records;
        const char* ans = "+------------------+------------------+\n"
            "|               id |              num |\n"
            "+------------------+------------------+\n"
            "|                1 |                1 |\n"
            "+------------------+------------------+\n"
            "Total record(s): 1\n";

        EXPECT_EQ(*result, *ans);

        exec_sql("commit;", result, &offset, &txn_id);
    });

    t0.join();
    t1.join();
}

TEST_F(ConcurrencyTest, UnrepeatableReadTest) {
    /**
     * pre: create table t1 (id int, num int);
     * pre: insert into t1 values(1, 1);
     * t1: begin;
     * t1: select * from t1 where id = 1;
     * t2: begin;
     * t2: update t1 set num = 2 where id = 1;
     * t1: select * from t1 where id = 1;
     * t1: commit;
     * t2: commit;
     */

    char* res = new char[BUFFER_LENGTH];
    int pre_offset;
    int pre_txn_id = INVALID_TXN_ID;
    exec_sql("create table t1 (id int, num int);", res, &pre_offset, &pre_txn_id);
    exec_sql("insert into t1 values(1, 1);", res, &pre_offset, &pre_txn_id);

    std::thread t0([&] {
        char *result = new char[BUFFER_LENGTH];
        txn_id_t txn_id = INVALID_TXN_ID;
        int offset;
        exec_sql("begin;", result, &offset, &txn_id);
        exec_sql("select * from t1 where id = 1;", result, &offset, &txn_id);
        char *first_result = new char[BUFFER_LENGTH];
        memcpy(first_result, result, offset);
        first_result[offset] = '\0';

        std::this_thread::sleep_for(std::chrono::milliseconds(200));

        exec_sql("select * from t1 where id = 1;", result, &offset, &txn_id);
        EXPECT_EQ(*first_result, *result);

        exec_sql("commit;", result, &offset, &txn_id);
    });

    std::thread t1([&] {
        char *result = new char[BUFFER_LENGTH];
        txn_id_t txn_id = INVALID_TXN_ID;
        int offset;
        // sleep thread1 to make thread0 obtain lock first;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        exec_sql("begin;", result, &offset, &txn_id);
        // this sentence
        exec_sql("update t1 set num = 2 where id = 1;", result, &offset, &txn_id);

        exec_sql("commit;", result, &offset, &txn_id);
    });

    t0.join();
    t1.join();
}