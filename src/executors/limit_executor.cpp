#include "executors/limit_executor.h"

namespace huadb {

    LimitExecutor::LimitExecutor(ExecutorContext &context, std::shared_ptr<const LimitOperator> plan,
                                 std::shared_ptr<Executor> child)
            : Executor(context, {std::move(child)}), plan_(std::move(plan)) {
        offset_ = plan_->limit_offset_.value_or(0);
        count_ = plan_->limit_count_.value_or(-1);
    }

    void LimitExecutor::Init() { children_[0]->Init(); }

    std::shared_ptr<Record> LimitExecutor::Next() {
        // 通过 plan_ 获取 limit 语句中的 offset 和 limit 值
        // LAB 4 BEGIN
        // 获取limit_count的引用
        while (offset_ > 0) {
            children_[0]->Next();
            offset_--;
        }

        auto record = std::make_shared<Record>();

        if (count_ > 0 && count_ != -1) {
            record = children_[0]->Next();
            count_--;
        } else if (count_ == -1) {
            record = children_[0]->Next();
        } else {
            return nullptr;
        }
        return record;
    }

}  // namespace huadb
