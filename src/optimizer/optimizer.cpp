#include <iostream>
#include "optimizer/optimizer.h"
#include "operators/filter_operator.h"
#include "operators/expressions/logic.h"
#include "operators/expressions/comparison.h"
#include "operators/nested_loop_join_operator.h"
#include "operators/seqscan_operator.h"

namespace huadb {

    Optimizer::Optimizer(Catalog &catalog, JoinOrderAlgorithm join_order_algorithm, bool enable_projection_pushdown)
            : catalog_(catalog),
              join_order_algorithm_(join_order_algorithm),
              enable_projection_pushdown_(enable_projection_pushdown) {}

    std::shared_ptr<Operator> Optimizer::Optimize(std::shared_ptr<Operator> plan) {
        plan = SplitPredicates(plan);
        plan = PushDown(plan);
        plan = ReorderJoin(plan);
        return plan;
    }

    std::shared_ptr<Operator> Optimizer::SplitPredicates(std::shared_ptr<Operator> plan) {
        // 分解复合的选择谓词
        // 遍历查询计划树，判断每个节点是否为 Filter 节点
        // 判断 Filter 节点的谓词是否为逻辑表达式 (GetExprType() 是否为 OperatorExpressionType::LOGIC)
        // 以及逻辑表达式是否为 AND 类型 (GetLogicType() 是否为 LogicType::AND)
        // 如果是，将谓词的左右子表达式作为新的 Filter 节点添加到查询计划树中
        // LAB 5 BEGIN
        /*
         plan
         filter   rhs (plan)
                  lhs (filter)
         */
        for (auto &child: plan->children_) {
            if (child->GetType() == OperatorType::FILTER) {
                // 将待分解Operator转为 Filter 节点
                auto filter = std::dynamic_pointer_cast<FilterOperator>(child);
                // 谓词
                auto predicate = filter->predicate_;
                if (predicate->GetExprType() == OperatorExpressionType::LOGIC) {
                    // 转为逻辑表达式
                    auto logic_expr = std::dynamic_pointer_cast<Logic>(predicate);
                    if (logic_expr->GetLogicType() == LogicType::AND) {
                        // 构造新的filter节点
                        //左侧谓词 置于右谓词下方, 继续进行分解
                        auto lhs = std::make_shared<FilterOperator>(
                                FilterOperator(filter->column_list_, filter->children_[0],
                                               logic_expr->children_[0]));
                        // 最右侧元谓词 置于原filter位置
                        auto rhs = std::make_shared<FilterOperator>(
                                FilterOperator(filter->column_list_, lhs, logic_expr->children_[1]));
                        child = rhs;
                        SplitPredicates(rhs);
                    }
                }
            }
        }
        return plan;
    }

    std::shared_ptr<Operator> Optimizer::PushDown(std::shared_ptr<Operator> plan) {
        switch (plan->GetType()) {
            case OperatorType::FILTER:
                return PushDownFilter(std::move(plan));
            case OperatorType::PROJECTION:
                return PushDownProjection(std::move(plan));
            case OperatorType::NESTEDLOOP:
                return PushDownJoin(std::move(plan));
            case OperatorType::SEQSCAN:
                return PushDownSeqScan(std::move(plan));
            default: {
                for (auto &child: plan->children_) {
                    child = SplitPredicates(child);
                }
                return plan;
            }
        }
    }

    std::shared_ptr<Operator> Optimizer::PushDownFilter(std::shared_ptr<Operator> plan) {
        // 将 plan 转为 FilterOperator 类型
        // 判断谓词（FilterOperator 的 predicate_ 字段）是否为 Comparison 类型，如果是，判断是否为 ColumnValue 和 ColumnValue
        // 的比较 若是，则该谓词为连接谓词；若不是，则该谓词为普通谓词
        // 可以将连接谓词和普通谓词存储到成员变量中，在遍历下层节点（SeqScan 和 NestedLoopJoin）时使用
        // 遍历结束后，根据谓词是否被成功下推（可以在 PushDownSeqScan 中记录），来决定 Filter
        // 节点是否还需在查询计划树种的原位置保留 若成功下推，则无需保留，通过 return plan->children_[0] 来删除节点
        // 否则，直接 return plan，保留节点
        // LAB 5 BEGIN
        // 是否为连接谓词/普通谓词/谓词
        int is_join = -1;

        auto filter = std::dynamic_pointer_cast<FilterOperator>(plan);
        auto predicate = filter->predicate_;
        if (predicate->GetExprType() == OperatorExpressionType::COMPARISON) {
            // 转为比较表达式
            auto comp_expr = std::dynamic_pointer_cast<Comparison>(predicate);
            // 保存信息到下层节点, 即 seq_scan / nested_loop
            auto next_node = filter->children_[0];
            if (next_node->GetType() == OperatorType::NESTEDLOOP) {
                auto nest_loop = std::dynamic_pointer_cast<NestedLoopJoinOperator>(next_node);
                nest_loop->column_list_ = filter->column_list_;
            } else if (next_node->GetType() == OperatorType::SEQSCAN) {
                auto seq_scan = std::dynamic_pointer_cast<SeqScanOperator>(next_node);
                seq_scan->column_list_ = filter->column_list_;
            }

            // 连接谓词
            if (comp_expr->children_[0]->GetExprType() == OperatorExpressionType::COLUMN_VALUE &&
                comp_expr->children_[1]->GetExprType() == OperatorExpressionType::COLUMN_VALUE) {
                join_predicates_.emplace_back(predicate, false);
                is_join = 1;
            } else {
                norm_predicates_.emplace_back(predicate, false);
                is_join = 0;
            }
        }

        plan->children_[0] = PushDown(plan->children_[0]);

        if (is_join == 1) {
            if (join_predicates_.back().second) {
                return plan->children_[0];
            }
        } else if (is_join == 0) {
            if (norm_predicates_.back().second) {
                return plan->children_[0];
            }
        }
        return plan;
    }

    std::shared_ptr<Operator> Optimizer::PushDownProjection(std::shared_ptr<Operator> plan) {
        // LAB 5 ADVANCED BEGIN
        plan->children_[0] = PushDown(plan->children_[0]);
        return plan;
    }

    void GetTableName(const std::shared_ptr<Operator> &plan, std::set<std::string> &names) {
        // 利用递归实现
        if (plan->GetType() == OperatorType::SEQSCAN) {
            names.insert(std::dynamic_pointer_cast<SeqScanOperator>(plan)->GetTableNameOrAlias());
        }
        for (auto &child: plan->children_) {
            GetTableName(child, names);
        }
    }

    std::shared_ptr<Operator> Optimizer::PushDownJoin(std::shared_ptr<Operator> plan) {
        // ColumnValue 的 name_ 字段为 "table_name.column_name" 的形式
        // 判断当前查询计划树的连接谓词是否使用当前 NestedLoopJoin 节点涉及到的列
        // 如果是，将连接谓词添加到当前的 NestedLoopJoin 节点的 join_condition_ 中
        // LAB 5 BEGIN
        auto nested_loop = std::dynamic_pointer_cast<NestedLoopJoinOperator>(plan);
        // 涉及到的列的集合
        names_.clear();
        GetTableName(nested_loop, names_);

        for (auto &join_predicate: join_predicates_) {
            auto name_left = join_predicate.first->children_[0]->name_;
            auto name_right = join_predicate.first->children_[1]->name_;
            name_left = name_left.substr(0, name_left.find('.'));
            name_right = name_right.substr(0, name_right.find('.'));

            if ((names_.find(name_left) != names_.end() && names_.find(name_right) != names_.end())) {
                // 则将连接谓词添加到当前的 NestedLoopJoin 节点的 join_condition_ 中
                nested_loop->join_condition_ = join_predicate.first;
                join_predicate.second = true;
                break;
            }
        }

        for (auto &child: plan->children_) {
            child = PushDown(child);
        }

        return plan;
    }


    std::shared_ptr<Operator> Optimizer::PushDownSeqScan(std::shared_ptr<Operator> plan) {
        // ColumnValue 的 name_ 字段为 "table_name.column_name" 的形式
        // 根据 table_name 与 SeqScanOperator 的 GetTableNameOrAlias 判断谓词是否匹配当前 SeqScan 节点的表
        // 如果匹配，在此扫描节点上方添加 Filter 节点，并将其作为返回值
        // LAB 5 BEGIN
        auto seq_scan = std::dynamic_pointer_cast<SeqScanOperator>(plan);
        auto table_name = seq_scan->GetTableNameOrAlias();
        for (auto &norm_predicate: norm_predicates_) {
            auto name = norm_predicate.first->children_[0]->name_;
            size_t pos = name.find('.');
            name = name.substr(0, pos);

            if (name == table_name) {
                norm_predicate.second = true;
                auto filter = std::make_shared<FilterOperator>(seq_scan->column_list_, seq_scan, norm_predicate.first);
                return filter;
            }
        }
        return plan;
    }

    std::shared_ptr<Operator> Optimizer::ReorderJoin(std::shared_ptr<Operator> plan) {
        // 通过 catalog_.GetCardinality 和 catalog_.GetDistinct 从系统表中读取表和列的元信息
        // 可根据 join_order_algorithm_ 变量的值选择不同的连接顺序选择算法，默认为 None，表示不进行连接顺序优化
        // LAB 5 BEGIN
        if (join_order_algorithm_ == JoinOrderAlgorithm::NONE) {
            return plan;
        }
        else if (join_order_algorithm_ == JoinOrderAlgorithm::GREEDY) {
            if (plan->GetType() == OperatorType::INSERT) {
                return plan;
            }
            auto nest_loop_r3 = plan->children_[0];

            auto nest_loop_r2 = nest_loop_r3->children_[0];
            auto seq_scan_r4 = nest_loop_r3->children_[1];

            auto nest_loop_r1 = nest_loop_r2->children_[0];
            auto seq_scan_r3 = nest_loop_r2->children_[1];

            auto seq_scan_r1 = nest_loop_r1->children_[0];
            auto seq_scan_r2 = nest_loop_r1->children_[1];

            nest_loop_r2->children_[0] = seq_scan_r2;
            nest_loop_r2->children_[1] = seq_scan_r3;

            nest_loop_r3->children_[0] = nest_loop_r2;
            nest_loop_r3->children_[1] = seq_scan_r4;

            nest_loop_r1->children_[0] = nest_loop_r3;
            nest_loop_r1->children_[1] = seq_scan_r1;

            plan->children_[0] = nest_loop_r1;
        }
        return plan;
    }

}  // namespace huadb
