set(SOURCES concurrency/lock_manager.cpp transaction_manager.cpp)
add_library(transaction STATIC ${SOURCES})
target_link_libraries(transaction system recovery pthread)

#add_executable(my_txn_manager_test my_txn_manager_test.cpp)
#target_link_libraries(my_txn_manager_test transaction execution parser gtest_main execution pthread planner analyze)
#
#add_executable(my_lock_manager_test my_lock_manager_test.cpp)
#target_link_libraries(my_lock_manager_test transaction execution gtest_main)
#
## concurrency_test
#add_executable(my_concurrency_test my_concurrency_test.cpp)
#target_link_libraries(my_concurrency_test transaction execution parser gtest_main execution pthread planner analyze)