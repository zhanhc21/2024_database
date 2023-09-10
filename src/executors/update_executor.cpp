#include "executors/update_executor.h"

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
    // 获取正确的锁，加锁失败时抛出异常
    // LAB 3 BEGIN
    table_->DeleteRecord(record->GetRid(), context_.GetXid());
    std::vector<Value> values;
    for (const auto &expr : plan_->update_exprs_) {
      values.push_back(expr->Evaluate(record));
    }
    auto new_record = std::make_shared<Record>(std::move(values));
    auto rid = table_->InsertRecord(std::move(new_record), context_.GetXid(), context_.GetCid());
    // 获取正确的锁，加锁失败时抛出异常
    // LAB 3 BEGIN
    count++;
  }
  finished_ = true;
  return std::make_shared<Record>(std::vector{Value(count)});
}

}  // namespace huadb
