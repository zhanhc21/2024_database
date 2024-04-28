#include "executors/update_executor.h"
#include "common/exceptions.h"

namespace huadb {

    UpdateExecutor::UpdateExecutor(ExecutorContext &context, std::shared_ptr<const UpdateOperator> plan,
                                   std::shared_ptr<Executor> child)
            : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

    void UpdateExecutor::Init() {
        children_[0]->Init();
        table_ = context_.GetCatalog().GetTable(plan_->GetTableOid());
    }

    std::shared_ptr<Record> UpdateExecutor::Next() {
        if (finished_) {
            return nullptr;
        }
        uint32_t count = 0;
        while (auto record = children_[0]->Next()) {
            std::vector<Value> values;
            for (const auto &expr : plan_->update_exprs_) {
                values.push_back(expr->Evaluate(record));
            }
            auto new_record = std::make_shared<Record>(std::move(values));
            // 通过 context_ 获取正确的锁，加锁失败时抛出异常
            // LAB 3 BEGIN
            auto &lock_manager = context_.GetLockManager();
            auto xid = context_.GetXid();
            auto oid = table_->GetOid();

            if (!lock_manager.LockTable(xid, LockType::IX, oid)) {
                throw DbException("update set table lock IX failed");
            }

            auto rid = table_->UpdateRecord(record->GetRid(), context_.GetXid(), context_.GetCid(), new_record, true);

            if (!lock_manager.LockRow(xid, LockType::X, oid, rid)) {
                throw DbException("update set row lock X failed");
            }

            if (!lock_manager.LockRow(xid, LockType::X, oid,record->GetRid())) {
                throw DbException("update set row lock X failed");
            }

            count++;
        }
        finished_ = true;
        return std::make_shared<Record>(std::vector{Value(count)});
    }

}  // namespace huadb
