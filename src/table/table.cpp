#include <iostream>
#include "table/table.h"

#include "table/table_page.h"
#include "storage/page_manager.h"

namespace huadb {

    Table::Table(BufferPool &buffer_pool, LogManager &log_manager, oid_t oid, oid_t db_oid, ColumnList column_list,
                 bool new_table, bool is_empty)
            : buffer_pool_(buffer_pool),
              log_manager_(log_manager),
              oid_(oid),
              db_oid_(db_oid),
              column_list_(std::move(column_list)) {

        auto page_manager = buffer_pool_.NewPage(db_oid_, oid_, 1000);
        PageManager Page(page_manager);
        Page.Init();

        if (new_table || is_empty) {
            first_page_id_ = NULL_PAGE_ID;
        } else {
            first_page_id_ = 0;
        }
    }

    Rid Table::InsertRecord(std::shared_ptr<Record> record, xid_t xid, cid_t cid, bool write_log) {
        if (record->GetSize() > MAX_RECORD_SIZE) {
            throw DbException("Record size too large: " + std::to_string(record->GetSize()));
        }
        // 当 write_log 参数为 true 时开启写日志功能
        // 在插入记录时增加写 InsertLog 过程
        // 在创建新的页面时增加写 NewPageLog 过程
        // 设置页面的 page lsn
        // LAB 2 BEGIN

        // 使用 buffer_pool_ 获取页面
        // 使用 TablePage 类操作记录页面
        // 遍历表的页面，判断页面是否有足够的空间插入记录，如果没有则通过 buffer_pool_ 创建新页面
        // 如果 first_page_id_ 为 NULL_PAGE_ID，说明表还没有页面，需要创建新页面
        // 创建新页面时需设置前一个页面的 next_page_id，并将新页面初始化
        // 找到空间足够的页面后，通过 TablePage 插入记录
        // 返回插入记录的 rid
        // LAB 1 BEGIN

        pageid_t current_page_id = 0;
        slotid_t slot_id = 0;

        auto page_manager = buffer_pool_.GetPage(db_oid_, oid_, 1000);
        PageManager PageManager(page_manager);

        if (first_page_id_ == NULL_PAGE_ID) {
            first_page_id_ = 0;
            PageManager.NewPage();
            PageManager.InsertRecord(record->GetSize());

            auto first_page = buffer_pool_.NewPage(db_oid_, oid_, first_page_id_);
            TablePage FirstPage(first_page);
            FirstPage.Init();
            slot_id = FirstPage.InsertRecord(record, xid, cid);
        }
        else {
            // 若满足条件
            auto page_id = PageManager.InsertRecord(record->GetSize());
            if (page_id != -1) {
                auto page = buffer_pool_.GetPage(db_oid_, oid_, page_id);
                TablePage Page(page);
                slot_id = Page.InsertRecord(record, xid, cid);

                current_page_id = page_id;
            }
            // 无可用页
            else {
                PageManager.NewPage();
                page_id = PageManager.InsertRecord(record->GetSize());

                auto page = buffer_pool_.GetPage(db_oid_, oid_, page_id - 1);
                TablePage Page(page);
                Page.SetNextPageId(page_id);

                auto new_page = buffer_pool_.NewPage(db_oid_, oid_, page_id);
                TablePage NewPage(new_page);
                NewPage.Init();

                slot_id = NewPage.InsertRecord(record, xid, cid);

                current_page_id = page_id;
            }

//            while (current_page_id != NULL_PAGE_ID) {
//                auto current_page = buffer_pool_.GetPage(db_oid_, oid_, current_page_id);
//                TablePage Page(current_page);
//
//                if (Page.GetFreeSpaceSize() >= record->GetSize()) {
//                    slot_id = Page.InsertRecord(record, xid, cid);
//                    break;
//                }
//                // 无可用页
//                if (Page.GetNextPageId() == NULL_PAGE_ID) {
//                    auto new_page = buffer_pool_.NewPage(db_oid_, oid_, current_page_id + 1);
//                    TablePage NewPage(new_page);
//                    NewPage.Init();
//                    Page.SetNextPageId(current_page_id + 1);
//                    slot_id = NewPage.InsertRecord(record, xid, cid);
//                    break;
//                }
//                current_page_id = Page.GetNextPageId();
//            }
        }
        return {current_page_id, slot_id};
    }

    void Table::DeleteRecord(const Rid &rid, xid_t xid, bool write_log) {
        // 增加写 DeleteLog 过程
        // 设置页面的 page lsn
        // LAB 2 BEGIN

        // 使用 TablePage 操作页面
        // LAB 1 BEGIN
        auto page = buffer_pool_.GetPage(db_oid_, oid_, rid.page_id_);
        TablePage Page(page);
        Page.DeleteRecord(rid.slot_id_, xid);
    }

    Rid Table::UpdateRecord(const Rid &rid, xid_t xid, cid_t cid, std::shared_ptr<Record> record, bool write_log) {
        DeleteRecord(rid, xid, write_log);
        return InsertRecord(record, xid, cid, write_log);
    }

    pageid_t Table::GetFirstPageId() const { return first_page_id_; }

    oid_t Table::GetOid() const { return oid_; }

    oid_t Table::GetDbOid() const { return db_oid_; }

    const ColumnList &Table::GetColumnList() const { return column_list_; }

}  // namespace huadb
