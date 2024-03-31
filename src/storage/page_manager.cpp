#include "page_manager.h"
#include "common/constants.h"
#include "table/table_page.h"

namespace huadb {

    PageManager::PageManager(std::shared_ptr<Page> page) : page_(page) {
        page_data_ = page->GetData();
        page_manager_ = reinterpret_cast<uint8_t *>(page_data_);
    }

    pageid_t PageManager::InsertRecord(db_size_t record_size) {
        pageid_t page_id = NULL_PAGE_ID;
        for (int i=0; i < page_num_; ++i) {
             if (page_manager_[i] >= record_size) {
                 page_id = i;
                 page_manager_[i] -= record_size;
                 break;
             }
        }
        return page_id;
    }

    void PageManager::Init() {
        page_num_ = 0;
        page_->SetDirty();
    }

    void PageManager::NewPage() {
        page_manager_[page_num_] = static_cast<uint8_t>(256);
        page_num_ += 1;
    }
}  // namespace huadb
