//
// Created by zhanhc on 2024/3/31.
//

#ifndef HUADB_PAGE_MANAGER_H
#define HUADB_PAGE_MANAGER_H
#pragma once
#include "common/types.h"
#include "storage/page.h"
#include <list>
#include <memory>


namespace huadb {

    class PageManager {
    public:
        explicit PageManager(std::shared_ptr<Page> page);
        pageid_t InsertRecord(db_size_t record_size);
        void NewPage();
        void Init();

    private:
        db_size_t page_num_;
        std::shared_ptr<Page> page_;
        char *page_data_;
        uint8_t *page_manager_;
    };

}  // namespace huadb


#endif //HUADB_PAGE_MANAGER_H
