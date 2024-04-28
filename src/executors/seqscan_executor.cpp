#include "executors/seqscan_executor.h"
#include <stdexcept>
#include "common/types.h"
#include "transaction/transaction_manager.h"

namespace huadb {

    SeqScanExecutor::SeqScanExecutor(ExecutorContext &context, std::shared_ptr<const SeqScanOperator> plan)
            : Executor(context, {}), plan_(std::move(plan)) {}

    void SeqScanExecutor::Init() {
        auto table = context_.GetCatalog().GetTable(plan_->GetTableOid());
        scan_ = std::make_unique<TableScan>(context_.GetBufferPool(), table, Rid{table->GetFirstPageId(), 0});
    }

    std::shared_ptr<Record> SeqScanExecutor::Next() {
        std::unordered_set<xid_t> active_xids;
        // 根据隔离级别，获取活跃事务的 xid（通过 context_ 获取需要的信息）
        // 通过 context_ 获取正确的锁，加锁失败时抛出异常
        // LAB 3 BEGIN

        auto xid = context_.GetXid();
        auto cid = context_.GetCid();
        auto iso_level = context_.GetIsolationLevel();
        auto &trans_manager = context_.GetTransactionManager();

        // 可重复读 / 串行化
        if (iso_level == IsolationLevel::REPEATABLE_READ || iso_level == IsolationLevel::SERIALIZABLE) {
            active_xids = trans_manager.GetSnapshot(xid);
        }
        // 读已提交
        else if (iso_level == IsolationLevel::READ_COMMITTED) {
            active_xids = trans_manager.GetActiveTransactions();
        }

        auto table = context_.GetCatalog().GetTable(plan_->GetTableOid());
        auto record = scan_->GetNextRecord(xid, iso_level, cid, active_xids);
        auto oid = table->GetOid();
        auto &lock_manager = context_.GetLockManager();

        // 表锁 IS
        if (!lock_manager.LockTable(xid, LockType::IS, oid)) {
            throw DbException("Set table lock IS failed");
        }

        return record;
    }

}  // namespace huadb
