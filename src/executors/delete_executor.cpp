#include "executors/delete_executor.h"
#include <exception>
#include "transaction/lock_manager.h"

namespace huadb {

    DeleteExecutor::DeleteExecutor(ExecutorContext &context, std::shared_ptr<const DeleteOperator> plan,
                                   std::shared_ptr<Executor> child)
            : Executor(context, {std::move(child)}), plan_(std::move(plan)) {
        table_ = context_.GetCatalog().GetTable(plan_->GetTableOid());
    }

    void DeleteExecutor::Init() { children_[0]->Init(); }

    std::shared_ptr<Record> DeleteExecutor::Next() {
        if (finished_) {
            return nullptr;
        }
        uint32_t count = 0;
        while (auto record = children_[0]->Next()) {
            // 通过 context_ 获取正确的锁，加锁失败时抛出异常
            // LAB 3 BEGIN
            auto &lock_manager = context_.GetLockManager();
            auto xid = context_.GetXid();
            auto oid = table_->GetOid();
            auto rid = record->GetRid();
//            // 表锁 IX
//            if (!lock_manager.LockTable(xid, LockType::IX, oid)) {
//                throw std::runtime_error("Set table lock IX failed");
//            }
//            // 行锁 X
//            if (!lock_manager.LockRow(xid, LockType::X, oid, rid)) {
//                throw std::runtime_error("Set row lock X failed");
//            }

            table_->DeleteRecord(rid, xid, true);
            count++;
        }
        finished_ = true;
        return std::make_shared<Record>(std::vector{Value(count)});
    }

}  // namespace huadb
