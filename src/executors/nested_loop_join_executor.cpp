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
        s_record_ = children_[1]->Next();

        if (r_record_ != nullptr) {
            r_value_size_ = r_record_->GetValues().size();
        }
        if (s_record_ != nullptr) {
            s_value_size_ = s_record_->GetValues().size();
        }

        r_table_.clear();
        s_table_.clear();

        while (r_record_ != nullptr) {
            r_table_.emplace_back(r_index_, false);
            r_record_ = children_[0]->Next();
            ++r_index_;
        }
        while (s_record_ != nullptr) {
            s_table_.emplace_back(s_index_, false);
            s_record_ = children_[1]->Next();
            ++s_index_;
        }

        s_index_ = 0;
        r_index_ = 0;
        children_[0]->Init();
        children_[1]->Init();
        r_record_ = children_[0]->Next();
        s_record_ = children_[1]->Next();
    }

    std::shared_ptr<Record> NestedLoopJoinExecutor::Next() {
        // 从 NestedLoopJoinOperator 中获取连接条件
        // 使用 OperatorExpression 的 EvaluateJoin 函数判断是否满足 join 条件
        // 使用 Record 的 Append 函数进行记录的连接
        // LAB 4 BEGIN
        auto join_condition = plan_->join_condition_;
        auto join_type = plan_->join_type_;

        switch (join_type) {
            case JoinType::INNER:
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
                            s_record_ = children_[1]->Next();
                            return result;
                        }
                        s_record_ = children_[1]->Next();
                    }
                    r_record_ = children_[0]->Next();
                }


            case JoinType::LEFT:
                while (true) {
                    if (r_record_ == nullptr) {
                        return nullptr;
                    }
                    while (true) {
                        if (s_record_ == nullptr) {
                            children_[1]->Init();
                            s_record_ = children_[1]->Next();
                            s_index_ = 0;
                            break;
                        }
                        if (join_condition->EvaluateJoin(r_record_, s_record_).GetValue<bool>()) {
                            auto result = std::make_shared<Record>(*r_record_);
                            result->Append(*s_record_);

                            // 标记已经被连接过
                            r_table_[r_index_].second = true;

                            s_record_ = children_[1]->Next();
                            ++s_index_;
                            return result;
                        }
                        s_record_ = children_[1]->Next();
                        ++s_index_;
                    }

                    if (!r_table_[r_index_].second && s_value_size_ != 0) {
                        auto result = std::make_shared<Record>(*r_record_);
                        std::vector<Value> null_values(s_value_size_, Value());
                        result->Append(Record(null_values));
                        r_record_ = children_[0]->Next();
                        ++r_index_;
                        return result;
                    }

                    r_record_ = children_[0]->Next();
                    ++r_index_;
                }


            case JoinType::RIGHT:
                while (true) {
                    if (r_record_ == nullptr) {
                        while (s_record_ != nullptr && r_value_size_ != 0) {
                            if (!s_table_[s_index_].second) {
                                std::vector<Value> null_values(r_value_size_, Value());
                                auto result = std::make_shared<Record>(Record(null_values));
                                result->Append(*s_record_);
                                result->SetValue(0, s_record_->GetValue(0));

                                // 标记已经被连接过
                                s_table_[s_index_].second = true;

                                s_record_ = children_[1]->Next();
                                ++s_index_;
                                return result;
                            }
                            s_record_ = children_[1]->Next();
                            ++s_index_;
                        }
                        return nullptr;
                    }
                    while (true) {
                        if (s_record_ == nullptr) {
                            children_[1]->Init();
                            s_record_ = children_[1]->Next();
                            s_index_ = 0;
                            break;
                        }
                        if (join_condition->EvaluateJoin(r_record_, s_record_).GetValue<bool>()) {
                            auto result = std::make_shared<Record>(*r_record_);
                            result->Append(*s_record_);

                            // 标记已经被连接过
                            s_table_[s_index_].second = true;

                            s_record_ = children_[1]->Next();
                            ++s_index_;
                            return result;
                        }
                        s_record_ = children_[1]->Next();
                        ++s_index_;
                    }

                    r_record_ = children_[0]->Next();
                    ++r_index_;
                }


            case JoinType::FULL:
                while (true) {
                    if (r_record_ == nullptr) {
                        while (s_record_ != nullptr && r_value_size_ != 0) {
                            if (!s_table_[s_index_].second) {
                                std::vector<Value> null_values(r_value_size_, Value());
                                auto result = std::make_shared<Record>(Record(null_values));
                                result->Append(*s_record_);
                                result->SetValue(0, s_record_->GetValue(0));

                                // 标记已经被连接过
                                s_table_[s_index_].second = true;

                                s_record_ = children_[1]->Next();
                                ++s_index_;
                                return result;
                            }
                            s_record_ = children_[1]->Next();
                            ++s_index_;
                        }
                        return nullptr;
                    }
                    while (true) {
                        if (s_record_ == nullptr) {
                            children_[1]->Init();
                            s_record_ = children_[1]->Next();
                            s_index_ = 0;
                            break;
                        }
                        if (join_condition->EvaluateJoin(r_record_, s_record_).GetValue<bool>()) {
                            auto result = std::make_shared<Record>(*r_record_);
                            result->Append(*s_record_);

                            // 标记已经被连接过
                            r_table_[r_index_].second = true;
                            s_table_[s_index_].second = true;

                            s_record_ = children_[1]->Next();
                            ++s_index_;
                            return result;
                        }
                        s_record_ = children_[1]->Next();
                        ++s_index_;
                    }

                    if (!r_table_[r_index_].second && s_value_size_ != 0) {
                        auto result = std::make_shared<Record>(*r_record_);
                        std::vector<Value> null_values(s_value_size_, Value());
                        result->Append(Record(null_values));
                        r_record_ = children_[0]->Next();
                        ++r_index_;
                        return result;
                    }

                    r_record_ = children_[0]->Next();
                    ++r_index_;
                }
        }
    }
}  // namespace huadb
