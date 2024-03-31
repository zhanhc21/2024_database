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

//    struct PageManager {
//        pageid_t page_id_;
//        db_size_t left_space_;
//
//        bool operator > (PageManager space_manager) const {
//            return this->left_space_ > space_manager.left_space_;
//        }
//    };
//
//    class PageManager : public Page{
//    public:
//        PageManager();
//        db_size_t GetMaxSpace();
//        pageid_t GetMaxSpacePage();
//        void NewPage(pageid_t page_id);
//        void InsertRecord(db_size_t record_size);
//        // void DeleteRecord(db_size_t record_size);
//        pageid_t GetPageNum();
//
//    private:
//        std::list<PageManager> page_manager_;
//    };
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
        db_size_t* page_manager_;
    };
}


#endif //HUADB_PAGE_MANAGER_H
