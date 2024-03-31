#include "page_manager.h"
#include "common/constants.h"
#include "table/table_page.h"

namespace huadb {
//    PageManager::PageManager() = default;
//
//    db_size_t PageManager::GetMaxSpace() {
//        if (page_manager_.empty()) {
//            return 0;
//        }
//        return page_manager_.front().left_space_;
//    }
//
//    pageid_t PageManager::GetMaxSpacePage() {
//        if (page_manager_.empty()) {
//            return -1;
//        }
//        return page_manager_.front().page_id_;
//    }
//
//    void PageManager::NewPage(pageid_t page_id) {
//        PageManager new_page {
//            page_id,
//            DB_PAGE_SIZE - PAGE_HEADER_SIZE - sizeof(Slot),
//        };
//        page_manager_.emplace_front(new_page);
//    }
//
//    bool Compare(PageManager a, PageManager b) {
//        return a > b;
//    }
//
//    void PageManager::InsertRecord(huadb::db_size_t record_size) {
//        page_manager_.front().left_space_ -= record_size;
//        // 降序排列
//        page_manager_.sort(Compare);
//    }
//
////    void PageManager::DeleteRecord(huadb::db_size_t record_size) {
////        page_manager_.front().left_space_ += record_size;
////        // 降序排列
////        page_manager_.sort(Compare);
////    }
//
//    pageid_t PageManager::GetPageNum() {
//        return page_manager_.size();
//    }

    PageManager::PageManager(std::shared_ptr<Page> page) : page_(page) {
        page_data_ = page->GetData();
        page_manager_ = reinterpret_cast<db_size_t *>(page_data_);
    }

    pageid_t PageManager::InsertRecord(db_size_t record_size) {
        pageid_t page_id = -1;
        for (int i=0; i<page_num_; ++i) {
             if (page_manager_[i * 4] >= record_size) {
                 page_id = i;
                 page_manager_[i * 4] -= record_size;
                 break;
             }
        }
        return page_id;
    }

    void PageManager::Init() {
        page_num_ = 0;
        for (int i = 0; i < 256; ++i) {
            page_manager_[i * 4] = 0;
        }
    }

    void PageManager::NewPage() {
        page_manager_[page_num_] = 256;
        page_num_+=1;
    }
}  // namespace huadb
