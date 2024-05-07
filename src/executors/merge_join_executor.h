#pragma once

#include <memory>
#include <vector>
#include "executors/executor.h"
#include "operators/merge_join_operator.h"
#include "table/record.h"

namespace huadb {

    class MergeJoinExecutor : public Executor {
    public:
        MergeJoinExecutor(ExecutorContext &context, std::shared_ptr<const MergeJoinOperator> plan,
                          std::shared_ptr<Executor> left, std::shared_ptr<Executor> right);

        void Init() override;

        std::shared_ptr<Record> Next() override;

    private:
        std::shared_ptr<const MergeJoinOperator> plan_;
        std::shared_ptr<Record> r_record_;
        std::shared_ptr<Record> s_record_;
        std::vector<std::shared_ptr<Record>> last_match_;
        int index_;
    };

}  // namespace huadb
