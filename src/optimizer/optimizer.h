#pragma once

#include <set>
#include "catalog/catalog.h"
#include "operators/operator.h"
#include "operators/expressions/expression.h"

namespace huadb {

    enum class JoinOrderAlgorithm {
        NONE, DP, GREEDY
    };
    static constexpr JoinOrderAlgorithm DEFAULT_JOIN_ORDER_ALGORITHM = JoinOrderAlgorithm::NONE;

    class Optimizer {
    public:
        Optimizer(Catalog &catalog, JoinOrderAlgorithm join_order_algorithm, bool enable_projection_pushdown);

        std::shared_ptr<Operator> Optimize(std::shared_ptr<Operator> plan);

    private:
        std::shared_ptr<Operator> SplitPredicates(std::shared_ptr<Operator> plan);

        std::shared_ptr<Operator> PushDown(std::shared_ptr<Operator> plan);

        std::shared_ptr<Operator> PushDownFilter(std::shared_ptr<Operator> plan);

        std::shared_ptr<Operator> PushDownProjection(std::shared_ptr<Operator> plan);

        std::shared_ptr<Operator> PushDownJoin(std::shared_ptr<Operator> plan);

        std::shared_ptr<Operator> PushDownSeqScan(std::shared_ptr<Operator> plan);

        std::shared_ptr<Operator> ReorderJoin(std::shared_ptr<Operator> plan);

        JoinOrderAlgorithm join_order_algorithm_;
        bool enable_projection_pushdown_;
        Catalog &catalog_;
        // 连接谓词
        std::vector<std::pair<std::shared_ptr<OperatorExpression>, bool>> join_predicates_;
        // 普通谓词
        std::vector<std::pair<std::shared_ptr<OperatorExpression>, bool>> norm_predicates_;

        std::set<std::string> names_;
    };

}  // namespace huadb
