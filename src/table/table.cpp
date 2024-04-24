#include <iostream>
#include "table/table.h"
#include "table/table_page.h"

namespace huadb {

    Table::Table(BufferPool &buffer_pool, LogManager &log_manager, oid_t oid, oid_t db_oid, ColumnList column_list,
                 bool new_table, bool is_empty)
            : buffer_pool_(buffer_pool),
              log_manager_(log_manager),
              oid_(oid),
              db_oid_(db_oid),
              column_list_(std::move(column_list)) {
        if (new_table || is_empty) {
            first_page_id_ = NULL_PAGE_ID;
        } else {
            first_page_id_ = 0;
        }
    }

    Rid Table::InsertRecord(const std::shared_ptr<Record>& record, xid_t xid, cid_t cid, bool write_log) {
        if (record->GetSize() > MAX_RECORD_SIZE) {
            throw DbException("Record size too large: " + std::to_string(record->GetSize()));
        }
        // 当 write_log 参数为 true 时开启写日志功能
        // 在插入记录时增加写 InsertLog 过程
        // 在创建新的页面时增加写 NewPageLog 过程
        // 设置页面的 page lsn

        pageid_t current_page_id = 0;
        slotid_t slot_id = 0;

        if (first_page_id_ == NULL_PAGE_ID) {
            first_page_id_ = 0;
            auto first_page = buffer_pool_.NewPage(db_oid_, oid_, first_page_id_);
            TablePage first_table_page(first_page);
            first_table_page.Init();
            slot_id = first_table_page.InsertRecord(record, xid, cid);

            if (write_log) {
                // 这里offset为插入record后的upper指针， 即指向当前记录的头部
                db_size_t offset = first_table_page.GetUpper();
                char *new_record = first_table_page.GetPageData() + offset;

                log_manager_.AppendNewPageLog(xid, oid_, NULL_PAGE_ID, first_page_id_);
                auto lsn = log_manager_.AppendInsertLog(xid, oid_, first_page_id_, slot_id, offset, record->GetSize(),
                                                        new_record);
                first_table_page.SetPageLSN(lsn);
            }

        } else {
            while (current_page_id != NULL_PAGE_ID) {
                auto current_page = buffer_pool_.GetPage(db_oid_, oid_, current_page_id);
                TablePage table_page(current_page);

                if (table_page.GetFreeSpaceSize() >= record->GetSize()) {
                    slot_id = table_page.InsertRecord(record, xid, cid);

                    if (write_log) {
                        db_size_t offset = table_page.GetUpper();
                        char *new_record = table_page.GetPageData() + offset;
                        auto lsn = log_manager_.AppendInsertLog(xid, oid_, current_page_id, slot_id, offset,
                                                                record->GetSize(), new_record);
                        table_page.SetPageLSN(lsn);
                    }

                    break;
                }
                // 无可用表
                if (table_page.GetNextPageId() == NULL_PAGE_ID) {
                    auto new_page = buffer_pool_.NewPage(db_oid_, oid_, current_page_id + 1);
                    TablePage new_table_page(new_page);
                    new_table_page.Init();
                    table_page.SetNextPageId(current_page_id + 1);
                    slot_id = new_table_page.InsertRecord(record, xid, cid);

                    if (write_log) {
                        db_size_t offset = new_table_page.GetUpper();
                        char *new_record = new_table_page.GetPageData() + offset;
                        log_manager_.AppendNewPageLog(xid, oid_, current_page_id, current_page_id + 1);
                        auto lsn = log_manager_.AppendInsertLog(xid, oid_, current_page_id + 1, slot_id, offset,
                                                                record->GetSize(), new_record);
                        new_table_page.SetPageLSN(lsn);
                    }

                    break;
                }
                current_page_id = table_page.GetNextPageId();
            }
        }
        return {current_page_id, slot_id};
    }

    void Table::DeleteRecord(const Rid &rid, xid_t xid, bool write_log) {
        // 增加写 DeleteLog 过程
        // 设置页面的 page lsn
        auto page = buffer_pool_.GetPage(db_oid_, oid_, rid.page_id_);
        TablePage table_page(page);
        table_page.DeleteRecord(rid.slot_id_, xid);

        if (write_log) {
            auto lsn = log_manager_.AppendDeleteLog(xid, oid_, rid.page_id_, rid.slot_id_);
            table_page.SetPageLSN(lsn);
        }
    }

    Rid Table::UpdateRecord(const Rid &rid, xid_t xid, cid_t cid, const std::shared_ptr<Record>& record, bool write_log) {
        DeleteRecord(rid, xid, write_log);
        return InsertRecord(record, xid, cid, write_log);
    }

    void Table::UpdateRecordInPlace(const Record &record) {
        auto rid = record.GetRid();
        auto table_page = std::make_unique<TablePage>(buffer_pool_.GetPage(db_oid_, oid_, rid.page_id_));
        table_page->UpdateRecordInPlace(record, rid.slot_id_);
    }

    pageid_t Table::GetFirstPageId() const { return first_page_id_; }

    oid_t Table::GetOid() const { return oid_; }

    oid_t Table::GetDbOid() const { return db_oid_; }

    const ColumnList &Table::GetColumnList() const { return column_list_; }

}  // namespace huadb
