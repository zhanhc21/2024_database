#pragma once

#include "executors/executor.h"
#include "operators/nested_loop_join_operator.h"

namespace huadb {

    class NestedLoopJoinExecutor : public Executor {
    public:
        NestedLoopJoinExecutor(ExecutorContext &context, std::shared_ptr<const NestedLoopJoinOperator> plan,
                               std::shared_ptr<Executor> left, std::shared_ptr<Executor> right);

        void Init() override;

        std::shared_ptr<Record> Next() override;

    private:
        std::shared_ptr<const NestedLoopJoinOperator> plan_;
        std::shared_ptr<Record> r_record_;
        std::shared_ptr<Record> s_record_;

        size_t r_value_size_ = 0;
        size_t s_value_size_ = 0;

        std::vector<std::pair<int, bool>> r_table_;
        std::vector<std::pair<int, bool>> s_table_;

        int r_index_ = 0;
        int s_index_ = 0;
    };

}  // namespace huadb
