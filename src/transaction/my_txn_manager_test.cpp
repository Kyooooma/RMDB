#include "execution/execution_manager.h"
#include "transaction_manager.h"
#include "gtest/gtest.h"
#include "optimizer/plan.h"
#include "optimizer/planner.h"
#include "portal.h"
#include "optimizer/optimizer.h"

#include "errors.h"
#include "analyze/analyze.h"


#define BUFFER_LENGTH 8192

class TransactionTest : public::testing::Test {
public:
    std::string db_name_ = "Txn_Test_DB";
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
    txn_id_t txn_id = INVALID_TXN_ID;
    char *result = new char[BUFFER_LENGTH];
    int offset;

public:
    // This function is called before every test.
    void SetUp() override {
        ::testing::Test::SetUp();
        // For each test, we create a new BufferPoolManager...
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
//         create db and open db
        if (sm_manager->is_dir(db_name_)) {
            sm_manager->drop_db(db_name_);
        }
        sm_manager->create_db(db_name_);
        sm_manager->open_db(db_name_);
    }

    // This function is called after every test.
    void TearDown() override {
        sm_manager->close_db();  // exit
        // sm_manager_->drop_db(db_name_);  // 若不删除数据库文件，则将保留最后一个测试点的数据库
    };

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

    // The below helper functions are useful for testing.
    void exec_sql(const std::string &sql) {
        YY_BUFFER_STATE yy_buffer = yy_scan_string(sql.c_str());
        assert(yyparse() == 0 && ast::parse_tree != nullptr);
        yy_delete_buffer(yy_buffer);
        memset(result, 0, BUFFER_LENGTH);
        offset = 0;
        Context *context = new Context(lock_manager.get(), log_manager.get(),
                                       nullptr, result, &offset);
        SetTransaction(&txn_id, context);
        std::shared_ptr<Query> query = analyze->do_analyze(ast::parse_tree);
        // 优化器
        std::shared_ptr<Plan> plan = optimizer->plan_query(query, context);
        // portal
        std::shared_ptr<PortalStmt> portalStmt = portal->start(plan, context);
        portal->run(portalStmt, ql_manager.get(), &txn_id, context);
        portal->drop();
        if (!context->txn_->get_txn_mode()) {
            txn_manager->commit(context->txn_, context->log_mgr_);
        }
    };
};

TEST_F(TransactionTest, BeginTest) {
    Transaction *txn = nullptr;
    txn = txn_manager->begin(txn, log_manager.get());
//
    EXPECT_EQ(txn_manager->txn_map.size(), 1);
    EXPECT_NE(txn, nullptr);
    EXPECT_EQ(txn->get_state(), TransactionState::DEFAULT);
}

// test commit
TEST_F(TransactionTest, CommitTest) {
    exec_sql("create table t1 (num int);");
    exec_sql("begin;");
    exec_sql("insert into t1 values(1);");
    exec_sql("insert into t1 values(2);");
    exec_sql("insert into t1 values(3);");
    exec_sql("update t1 set num = 4 where num = 1;");
    exec_sql("delete from t1 where num = 3;");
    exec_sql("commit;");
    exec_sql("select * from t1;");
    const char *str = "+------------------+\n"
        "|              num |\n"
        "+------------------+\n"
        "|                4 |\n"
        "|                2 |\n"
        "+------------------+\n"
        "Total record(s): 2\n";
    EXPECT_STREQ(result, str);
    // there should be 3 transactions
    EXPECT_EQ(txn_manager->get_next_txn_id(), 3);
    Transaction *txn = txn_manager->get_transaction(1);
    EXPECT_EQ(txn->get_state(), TransactionState::COMMITTED);
}

// test abort
TEST_F(TransactionTest, AbortTest) {
    exec_sql("create table t1 (num int);");
    exec_sql("begin;");
    exec_sql("insert into t1 values(1);");
    exec_sql("insert into t1 values(2);");
    exec_sql("insert into t1 values(3);");
    exec_sql("update t1 set num = 4 where num = 1;");
    exec_sql("delete from t1 where num = 3;");
    exec_sql("abort;");
    exec_sql("select * from t1;");
    const char * str = "+------------------+\n"
        "|              num |\n"
        "+------------------+\n"
        "+------------------+\n"
        "Total record(s): 0\n";
    EXPECT_STREQ(result, str);
    EXPECT_EQ(txn_manager->get_next_txn_id(), 3);
    Transaction *txn = txn_manager->get_transaction(1);
    EXPECT_EQ(txn->get_state(), TransactionState::ABORTED);
}

