#include <iostream>
#include "executors/nested_loop_join_executor.h"

namespace huadb {

    NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext &context,
                                                   std::shared_ptr<const NestedLoopJoinOperator> plan,
                                                   std::shared_ptr<Executor> left, std::shared_ptr<Executor> right)
            : Executor(context, {std::move(left), std::move(right)}), plan_(std::move(plan)) {}

    void NestedLoopJoinExecutor::Init() {
        children_[0]->Init();
        children_[1]->Init();
        r_record_ = children_[0]->Next();
    }

    std::shared_ptr<Record> NestedLoopJoinExecutor::Next() {
        // 从 NestedLoopJoinOperator 中获取连接条件
        // 使用 OperatorExpression 的 EvaluateJoin 函数判断是否满足 join 条件
        // 使用 Record 的 Append 函数进行记录的连接
        // LAB 4 BEGIN
        auto join_condition = plan_->join_condition_;
        s_record_ = children_[1]->Next();

        while (true) {
            if (r_record_ == nullptr) {
                children_[0]->Init();
                children_[1]->Init();
                return nullptr;
            }

            while (true) {
                if (s_record_ == nullptr) {
                    children_[1]->Init();
                    s_record_ = children_[1]->Next();
                    break;
                }

                if (join_condition->EvaluateJoin(r_record_, s_record_).GetValue<bool>()) {
                    auto result = std::make_shared<Record>(*r_record_);
                    result->Append(*s_record_);
                    return result;
                }

                s_record_ = children_[1]->Next();
            }

            r_record_ = children_[0]->Next();
        }
    }
}  // namespace huadb
