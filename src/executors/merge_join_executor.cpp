#include <iostream>
#include <memory>
#include "executors/merge_join_executor.h"
#include "table/record.h"

namespace huadb {

    MergeJoinExecutor::MergeJoinExecutor(ExecutorContext &context, std::shared_ptr<const MergeJoinOperator> plan,
                                         std::shared_ptr<Executor> left, std::shared_ptr<Executor> right)
            : Executor(context, {std::move(left), std::move(right)}), plan_(std::move(plan)) {
        index_ = 0;
    }

    void MergeJoinExecutor::Init() {
        children_[0]->Init();
        children_[1]->Init();
        r_record_ = children_[0]->Next();
        s_record_ = children_[1]->Next();
    }

    std::shared_ptr<Record> MergeJoinExecutor::Next() {
        // LAB 4 BEGIN
        auto left_key = plan_->left_key_;
        auto right_key = plan_->right_key_;

        while (!last_match_.empty()) {
            if (index_ < last_match_.size()) {
                auto result = std::make_shared<Record>(*r_record_);
                result->Append(*last_match_[index_]);
                ++index_;
                return result;
            }

            auto last_r_record = r_record_;
            r_record_ = children_[0]->Next();
            if (r_record_ == nullptr || !left_key->Evaluate(r_record_).Equal(left_key->Evaluate(last_r_record))) {
                last_match_.clear();
            }

            index_ = 0;
        }

        while (r_record_ != nullptr && s_record_ != nullptr) {
            auto r_record_value = left_key->Evaluate(r_record_);
            auto s_record_value = right_key->Evaluate(s_record_);

            // r > s
            while (r_record_value.Greater(s_record_value)) {
                s_record_ = children_[1]->Next();
                if (s_record_ == nullptr) {
                    return nullptr;
                }
                s_record_value = right_key->Evaluate(s_record_);
           }

            // r < s
            while (r_record_value.Less(s_record_value)) {
                r_record_ = children_[0]->Next();
                if (r_record_ == nullptr) {
                    return nullptr;
                }
                r_record_value = left_key->Evaluate(r_record_);
            }

            // 如果R，S中没有重复元组，只需将(r,s)放入结果
            // 如果R，S中有重复元组，则需要找到所有满足条件的元组对
            if (r_record_value.Equal(s_record_value)) {
                auto result = std::make_shared<Record>(*r_record_);
                result->Append(*s_record_);

                last_match_.emplace_back(s_record_);
                s_record_ = children_[1]->Next();
                while (s_record_ != nullptr && right_key->Evaluate(s_record_).Equal(r_record_value)) {
                    last_match_.emplace_back(s_record_);
                    s_record_ = children_[1]->Next();
                }

                ++index_;
                return result;
            }
        }

        children_[0]->Init();
        children_[1]->Init();
        return nullptr;
    }

}  // namespace huadb
