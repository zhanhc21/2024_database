#include "executors/lock_rows_executor.h"
#include "common/exceptions.h"

namespace huadb {

    LockRowsExecutor::LockRowsExecutor(ExecutorContext &context, std::shared_ptr<const LockRowsOperator> plan,
                                       std::shared_ptr<Executor> child)
            : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

    void LockRowsExecutor::Init() { children_[0]->Init(); }

    std::shared_ptr<Record> LockRowsExecutor::Next() {
        auto record = children_[0]->Next();
        if (record == nullptr) {
            return nullptr;
        }
        // 根据 plan_ 的 lock type 获取正确的锁，加锁失败时抛出异常
        // LAB 3 BEGIN
        auto oid = plan_->GetOid();
        auto lock_type = plan_->GetLockType();
        auto xid = context_.GetXid();
        auto rid = record->GetRid();
        auto &lock_manager = context_.GetLockManager();

        if (lock_type == SelectLockType::SHARE) {
            if (!lock_manager.LockRow(xid, LockType::S, oid, rid)) {
                throw DbException("set lock s failed");
            }
        } else if (lock_type == SelectLockType::UPDATE) {
            if (!lock_manager.LockRow(xid, LockType::X, oid, rid)) {
                throw DbException("set lock x failed");
            }
        }

        return record;
    }

}  // namespace huadb
