#include "table/table_scan.h"

#include "table/table_page.h"

namespace huadb {

    TableScan::TableScan(BufferPool &buffer_pool, std::shared_ptr<Table> table, Rid rid)
            : buffer_pool_(buffer_pool), table_(std::move(table)), rid_(rid) {}

    std::shared_ptr<Record> TableScan::GetNextRecord(xid_t xid, IsolationLevel isolation_level, cid_t cid,
                                                     const std::unordered_set<xid_t> &active_xids) {
        // 根据事务隔离级别及活跃事务集合，判断记录是否可见
        // LAB 3 BEGIN

        // 每次调用读取一条记录
        // 读取时更新 rid_ 变量，避免重复读取
        // 扫描结束时，返回空指针
        // 注意处理扫描空表的情况（rid_.page_id_ 为 NULL_PAGE_ID）
        // LAB 1 BEGIN
        if (rid_.page_id_ == NULL_PAGE_ID) {
            return nullptr;
        }

        std::shared_ptr<Record> record = nullptr;

        while (true) {
            auto current_page = buffer_pool_.GetPage(table_->GetDbOid(), table_->GetOid(), rid_.page_id_);
            TablePage Page(current_page);

            if (rid_.slot_id_ < Page.GetRecordCount()) {
                record = Page.GetRecord(rid_, table_ -> GetColumnList());
                rid_.slot_id_ += 1;
                if (record->IsDeleted()) {
                    continue;
                }
                break;
            }
            // 切换页面
            else if (Page.GetNextPageId() != NULL_PAGE_ID) {
                rid_.page_id_ = Page.GetNextPageId();
                rid_.slot_id_ = 0;
                current_page = buffer_pool_.GetPage(table_->GetDbOid(), table_->GetOid(), rid_.page_id_);
                new (&Page) TablePage(current_page);

                record = Page.GetRecord(rid_, table_->GetColumnList());
                rid_.slot_id_ += 1;
                if (record->IsDeleted()) {
                    continue;
                }
                break;
            }
            else {
                rid_.page_id_ = NULL_PAGE_ID;
                rid_.slot_id_ = 0;
                return nullptr;
            }
        }
        return record;
    }

}  // namespace huadb
