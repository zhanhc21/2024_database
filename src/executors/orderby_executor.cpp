#include "executors/orderby_executor.h"
#include <memory>
#include "binder/order_by.h"
#include "common/value.h"

namespace huadb {
    bool Less(const std::pair<std::shared_ptr<Record>, Value> &record1,
              const std::pair<std::shared_ptr<Record>, Value> &record2) {
        return record1.second.Less(record2.second);
    }

    bool Greater(const std::pair<std::shared_ptr<Record>, Value> &record1,
                 const std::pair<std::shared_ptr<Record>, Value> &record2) {
        return record1.second.Greater(record2.second);
    }

    OrderByExecutor::OrderByExecutor(ExecutorContext &context, std::shared_ptr<const OrderByOperator> plan,
                                     std::shared_ptr<Executor> child)
            : Executor(context, {std::move(child)}), plan_(std::move(plan)), index_(0) {}

    void OrderByExecutor::Init() {
        children_[0]->Init();
        std::shared_ptr<Record> record = children_[0]->Next();

        while (record != nullptr) {
            sorted_records_.emplace_back(record, Value());
            record = children_[0]->Next();
        }

        size_t count = 1;
        for (const auto &order_by: plan_->order_bys_) {
            const auto order_type = order_by.first;
            const auto &op_expr = order_by.second;

            // 设置Value
            for (auto &iter: sorted_records_) {
                iter.second = op_expr->Evaluate(iter.first);
            }

            if (count == 1) {
                if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
                    std::sort(sorted_records_.begin(), sorted_records_.end(), Less);
                } else if (order_type == OrderByType::DESC) {
                    std::sort(sorted_records_.begin(), sorted_records_.end(), Greater);
                }
            } else {
                if (order_type == OrderByType::ASC || order_type == OrderByType::DEFAULT) {
                    for (int i = 1; i < indices_.size(); ++i) {
                        std::sort(sorted_records_.begin() + indices_[i - 1], sorted_records_.begin() + indices_[i],
                                  Less);
                    }
                    std::sort(sorted_records_.begin() + indices_.back(), sorted_records_.end(), Less);
                } else if (order_type == OrderByType::DESC) {
                    for (int i = 1; i < indices_.size(); ++i) {
                        std::sort(sorted_records_.begin() + indices_[i - 1], sorted_records_.begin() + indices_[i],
                                  Greater);
                    }
                    std::sort(sorted_records_.begin() + indices_.back(), sorted_records_.end(), Greater);
                }
            }

            indices_.clear();
            indices_.emplace_back(0);
            for (int i = 1; i < sorted_records_.size(); ++i) {
                if (!sorted_records_[i].second.Equal(sorted_records_[i - 1].second)) {
                    indices_.emplace_back(i);
                }
            }

            ++count;
        }
    }

    std::shared_ptr<Record> OrderByExecutor::Next() {
        // 可以使用 STL 的 sort 函数
        // 通过 OperatorExpression 的 Evaluate 函数获取 Value 的值
        // 通过 Value 的 Less, Equal, Greater 函数比较 Value 的值
        // LAB 4 BEGIN
        while (index_ != sorted_records_.size()) {
            auto record = sorted_records_[index_].first;
            ++index_;
            return record;
        }
        return nullptr;
    }
}  // namespace huadb
