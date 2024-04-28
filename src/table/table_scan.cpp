#include <iostream>
#include "common/constants.h"
#include "table/table_scan.h"
#include "common/types.h"
#include "table/table_page.h"
#include "transaction/transaction_manager.h"

namespace huadb {

    bool IsVisible(IsolationLevel iso_level, xid_t xid, cid_t cid, const std::unordered_set<xid_t> &active_xids, const std::shared_ptr<Record>& record) {
        bool visible = true;
        xid_t record_insert_xid = record->GetXmin();
        xid_t record_delete_xid = record->GetXmax();
        cid_t record_insert_cid = record->GetCid();

        if (iso_level == IsolationLevel::REPEATABLE_READ || iso_level == IsolationLevel::SERIALIZABLE) {
            // 删除
            if (record->IsDeleted() && active_xids.find(record_delete_xid) == active_xids.end() && record_delete_xid <= xid) {
                visible = false;
            }
            // 脏读 不可重复读
            if (active_xids.find(record_insert_xid) != active_xids.end() || record_insert_xid > xid) {
                visible = false;
            }
        } else if (iso_level == IsolationLevel::READ_COMMITTED) {
            // 删除
            if (record->IsDeleted() && (active_xids.find(record_delete_xid) == active_xids.end() || xid == record_delete_xid)) {
                visible = false;
            }
            // 脏读
            if (active_xids.find(record_insert_xid) != active_xids.end() && record_insert_xid != xid) {
                visible = false;
            }
        }
        // 万圣节问题
        if (record_insert_xid == xid && record_insert_cid == cid) {
            visible = false;
        }
        return visible;
    }

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
        if (rid_.page_id_ == NULL_PAGE_ID) {
            return nullptr;
        }

        std::shared_ptr<Record> record = nullptr;

        while (true) {
            auto current_page = buffer_pool_.GetPage(table_->GetDbOid(), table_->GetOid(), rid_.page_id_);
            TablePage table_page(current_page);
            if (rid_.slot_id_ < table_page.GetRecordCount()) {
                record = table_page.GetRecord(rid_, table_ -> GetColumnList());
                rid_.slot_id_ += 1;

                // 加入可见性判断
                if (!IsVisible(isolation_level, xid, cid, active_xids, record)) {
                    continue;
                }
                break;
            }
            // 切换页面
            else if (table_page.GetNextPageId() != NULL_PAGE_ID) {
                rid_.page_id_ = table_page.GetNextPageId();
                rid_.slot_id_ = 0;
                current_page = buffer_pool_.GetPage(table_->GetDbOid(), table_->GetOid(), rid_.page_id_);
                new (&table_page) TablePage(current_page);

                record = table_page.GetRecord(rid_, table_->GetColumnList());
                rid_.slot_id_ += 1;

                // 加入可见性判断
                if (!IsVisible(isolation_level, xid, cid, active_xids, record)) {
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
