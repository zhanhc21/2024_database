#include "log/log_manager.h"

#include "common/exceptions.h"
#include "log/log_records/log_records.h"
#include "table/table_page.h"

namespace huadb {

    LogManager::LogManager(Disk &disk, TransactionManager &transaction_manager, lsn_t next_lsn)
            : disk_(disk), transaction_manager_(transaction_manager), next_lsn_(next_lsn), flushed_lsn_(next_lsn - 1) {}

    void LogManager::SetBufferPool(std::shared_ptr<BufferPool> buffer_pool) { buffer_pool_ = std::move(buffer_pool); }

    void LogManager::SetCatalog(std::shared_ptr<Catalog> catalog) { catalog_ = std::move(catalog); }

    lsn_t LogManager::GetNextLSN() const { return next_lsn_; }

    void LogManager::Clear() {
        std::unique_lock lock(log_buffer_mutex_);
        log_buffer_.clear();
    }

    void LogManager::Flush() { Flush(NULL_LSN); }

    void LogManager::SetDirty(oid_t oid, pageid_t page_id, lsn_t lsn) {
        if (dpt_.find({oid, page_id}) == dpt_.end()) {
            dpt_[{oid, page_id}] = lsn;
        }
    }

    lsn_t LogManager::AppendInsertLog(xid_t xid, oid_t oid, pageid_t page_id, slotid_t slot_id, db_size_t offset,
                                      db_size_t size, char *new_record) {
        if (att_.find(xid) == att_.end()) {
            throw DbException(std::to_string(xid) + " does not exist in att (in AppendInsertLog)");
        }
        auto log = std::make_shared<InsertLog>(NULL_LSN, xid, att_.at(xid), oid, page_id, slot_id, offset, size,
                                               new_record);
        lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
        log->SetLSN(lsn);
        att_[xid] = lsn;
        {
            std::unique_lock lock(log_buffer_mutex_);
            log_buffer_.push_back(std::move(log));
        }
        if (dpt_.find({oid, page_id}) == dpt_.end()) {
            dpt_[{oid, page_id}] = lsn;
        }
        return lsn;
    }

    lsn_t LogManager::AppendDeleteLog(xid_t xid, oid_t oid, pageid_t page_id, slotid_t slot_id) {
        if (att_.find(xid) == att_.end()) {
            throw DbException(std::to_string(xid) + " does not exist in att (in AppendDeleteLog)");
        }
        auto log = std::make_shared<DeleteLog>(NULL_LSN, xid, att_.at(xid), oid, page_id, slot_id);
        lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
        log->SetLSN(lsn);
        att_[xid] = lsn;
        {
            std::unique_lock lock(log_buffer_mutex_);
            log_buffer_.push_back(std::move(log));
        }
        if (dpt_.find({oid, page_id}) == dpt_.end()) {
            dpt_[{oid, page_id}] = lsn;
        }
        return lsn;
    }

    lsn_t LogManager::AppendNewPageLog(xid_t xid, oid_t oid, pageid_t prev_page_id, pageid_t page_id) {
        if (xid != DDL_XID && att_.find(xid) == att_.end()) {
            throw DbException(std::to_string(xid) + " does not exist in att (in AppendNewPageLog)");
        }
        xid_t log_xid;
        if (xid == DDL_XID) {
            log_xid = NULL_XID;
        } else {
            log_xid = att_.at(xid);
        }
        auto log = std::make_shared<NewPageLog>(NULL_LSN, xid, log_xid, oid, prev_page_id, page_id);
        lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
        log->SetLSN(lsn);

        if (xid != DDL_XID) {
            att_[xid] = lsn;
        }
        {
            std::unique_lock lock(log_buffer_mutex_);
            log_buffer_.push_back(std::move(log));
        }
        if (dpt_.find({oid, page_id}) == dpt_.end()) {
            dpt_[{oid, page_id}] = lsn;
        }
        if (prev_page_id != NULL_PAGE_ID && dpt_.find({oid, prev_page_id}) == dpt_.end()) {
            dpt_[{oid, prev_page_id}] = lsn;
        }
        return lsn;
    }

    lsn_t LogManager::AppendBeginLog(xid_t xid) {
        if (att_.find(xid) != att_.end()) {
            throw DbException(std::to_string(xid) + " already exists in att");
        }
        auto log = std::make_shared<BeginLog>(NULL_LSN, xid, NULL_LSN);
        lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
        log->SetLSN(lsn);
        att_[xid] = lsn;
        {
            std::unique_lock lock(log_buffer_mutex_);
            log_buffer_.push_back(std::move(log));
        }
        return lsn;
    }

    lsn_t LogManager::AppendCommitLog(xid_t xid) {
        if (att_.find(xid) == att_.end()) {
            throw DbException(std::to_string(xid) + " does not exist in att (in AppendCommitLog)");
        }
        auto log = std::make_shared<CommitLog>(NULL_LSN, xid, att_.at(xid));
        lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
        log->SetLSN(lsn);
        {
            std::unique_lock lock(log_buffer_mutex_);
            log_buffer_.push_back(std::move(log));
        }
        Flush(lsn);
        att_.erase(xid);
        return lsn;
    }

    lsn_t LogManager::AppendRollbackLog(xid_t xid) {
        if (att_.find(xid) == att_.end()) {
            throw DbException(std::to_string(xid) + " does not exist in att (in AppendRollbackLog)");
        }
        auto log = std::make_shared<RollbackLog>(NULL_LSN, xid, att_.at(xid));
        lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
        log->SetLSN(lsn);
        {
            std::unique_lock lock(log_buffer_mutex_);
            log_buffer_.push_back(std::move(log));
        }
        Flush(lsn);
        att_.erase(xid);
        return lsn;
    }

    lsn_t LogManager::Checkpoint(bool async) {
        auto begin_checkpoint_log = std::make_shared<BeginCheckpointLog>(NULL_LSN, NULL_XID, NULL_LSN);
        lsn_t begin_lsn = next_lsn_.fetch_add(begin_checkpoint_log->GetSize(), std::memory_order_relaxed);
        begin_checkpoint_log->SetLSN(begin_lsn);
        {
            std::unique_lock lock(log_buffer_mutex_);
            log_buffer_.push_back(std::move(begin_checkpoint_log));
        }

        auto end_checkpoint_log = std::make_shared<EndCheckpointLog>(NULL_LSN, NULL_XID, NULL_LSN, att_, dpt_);
        lsn_t end_lsn = next_lsn_.fetch_add(end_checkpoint_log->GetSize(), std::memory_order_relaxed);
        end_checkpoint_log->SetLSN(end_lsn);
        {
            std::unique_lock lock(log_buffer_mutex_);
            log_buffer_.push_back(std::move(end_checkpoint_log));
        }
        Flush(end_lsn);
        std::ofstream out(MASTER_RECORD_NAME);
        out << begin_lsn;
        return end_lsn;
    }

    void LogManager::FlushPage(oid_t table_oid, pageid_t page_id, lsn_t page_lsn) {
        Flush(page_lsn);
        dpt_.erase({table_oid, page_id});
    }

    void LogManager::Rollback(xid_t xid) {
        // 在 att_ 中查找事务 xid 的最后一条日志的 lsn
        // 依次获取 lsn 的 prev_lsn_，直到 NULL_LSN
        // 根据 lsn 和 flushed_lsn_ 的大小关系，判断日志在 buffer 中还是在磁盘中
        // 若日志在 buffer 中，通过 log_buffer_ 获取日志
        // 若日志在磁盘中，通过 disk_ 读取日志，count 参数可设置为 MAX_LOG_SIZE
        // 通过 LogRecord::DeserializeFrom 函数解析日志
        // 调用日志的 Undo 函数

        lsn_t lsn = att_.find(xid)->second;
        while (lsn != NULL_LSN) {
            // log buffer
            if (lsn > flushed_lsn_) {
                for (const auto &record: log_buffer_) {
                    if (record->GetLSN() == lsn) {
                        lsn = record->GetPrevLSN();
                        record->Undo(*buffer_pool_, *catalog_, *this, lsn);
                        break;
                    }
                }
            }
            // disk
            else {
                char *record_data = static_cast<char*>(malloc(MAX_LOG_SIZE * sizeof(char)));
                disk_.ReadLog(lsn, MAX_LOG_SIZE, record_data);
                auto record = LogRecord::DeserializeFrom(lsn, record_data);
                lsn = record->GetPrevLSN();
                record->Undo(*buffer_pool_, *catalog_, *this, lsn);
                delete record_data;
            }
        }
    }

    void LogManager::Recover() {
        Analyze();
        Redo();
        Undo();
    }

    void LogManager::IncrementRedoCount() { redo_count_++; }

    uint32_t LogManager::GetRedoCount() const { return redo_count_; }

    void LogManager::Flush(lsn_t lsn) {
        size_t max_log_size = 0;
        lsn_t max_lsn = NULL_LSN;
        {
            std::unique_lock lock(log_buffer_mutex_);
            for (auto iterator = log_buffer_.cbegin(); iterator != log_buffer_.cend();) {
                const auto &log_record = *iterator;
                // 如果 lsn 为 NULL_LSN，表示 log_buffer_ 中所有日志都需要刷盘
                if (lsn != NULL_LSN && log_record->GetLSN() > lsn) {
                    iterator++;
                    continue;
                }
                auto log_size = log_record->GetSize();
                auto log = std::make_unique<char[]>(log_size);
                log_record->SerializeTo(log.get());
                disk_.WriteLog(log_record->GetLSN(), log_size, log.get());
                if (max_lsn == NULL_LSN || log_record->GetLSN() > max_lsn) {
                    max_lsn = log_record->GetLSN();
                    max_log_size = log_size;
                }
                iterator = log_buffer_.erase(iterator);
            }
        }
        // 如果 max_lsn 为 NULL_LSN，表示没有日志刷盘
        // 如果 flushed_lsn_ 为 NULL_LSN，表示还没有日志刷过盘
        if (max_lsn != NULL_LSN && (flushed_lsn_ == NULL_LSN || max_lsn > flushed_lsn_)) {
            flushed_lsn_ = max_lsn;
            lsn_t next_lsn = FIRST_LSN;
            if (disk_.FileExists(NEXT_LSN_NAME)) {
                std::ifstream in(NEXT_LSN_NAME);
                in >> next_lsn;
            }
            if (flushed_lsn_ + max_log_size > next_lsn) {
                std::ofstream out(NEXT_LSN_NAME);
                out << (flushed_lsn_ + max_log_size);
            }
        }
    }

    void LogManager::Analyze() {
        // 恢复 Master Record 等元信息
        // 恢复故障时正在使用的数据库
        if (disk_.FileExists(NEXT_LSN_NAME)) {
            std::ifstream in(NEXT_LSN_NAME);
            lsn_t next_lsn;
            in >> next_lsn;
            next_lsn_ = next_lsn;
        } else {
            next_lsn_ = FIRST_LSN;
        }
        flushed_lsn_ = next_lsn_ - 1;
        lsn_t checkpoint_lsn = 0;

        if (disk_.FileExists(MASTER_RECORD_NAME)) {
            std::ifstream in(MASTER_RECORD_NAME);
            in >> checkpoint_lsn;
        }
        // 根据 Checkpoint 日志恢复脏页表、活跃事务表等元信息
        // 必要时调用 transaction_manager_.SetNextXid 来恢复事务 id
        // LAB 2 BEGIN

        lsn_t lsn = checkpoint_lsn;
        // 重做阶段开始位置
        min_rec_lsn_ = checkpoint_lsn;
        char *record_data = static_cast<char*>(malloc(MAX_LOG_SIZE * sizeof(char)));

        while (lsn < next_lsn_) {
            disk_.ReadLog(lsn, MAX_LOG_SIZE, record_data);
            auto record = LogRecord::DeserializeFrom(lsn, record_data);

            // 检查点结束记录
            if (record->GetType() == LogType::END_CHECKPOINT) {
                auto end_checkpoint = std::dynamic_pointer_cast<EndCheckpointLog>(record);
                att_ = end_checkpoint->GetATT();
                dpt_ = end_checkpoint->GetDPT();
                break;
            }

            lsn += record->GetSize();
        }

        lsn = checkpoint_lsn;

        while (lsn < next_lsn_) {
            disk_.ReadLog(lsn, MAX_LOG_SIZE, record_data);
            auto record = LogRecord::DeserializeFrom(lsn, record_data);
            xid_t xid = record->GetXid();

            // 更新活跃事务表
            if (record->GetType() == LogType::INSERT || record->GetType() == LogType::DELETE || record->GetType() == LogType::NEW_PAGE) {
                att_[xid] = lsn;
            }
            // 事务结束记录
            if (record->GetType() == LogType::COMMIT) {
                if (att_.find(xid) != att_.end()) {
                    att_.erase(xid);
                }
            }

            if (xid > transaction_manager_.GetNextXid()) {
                transaction_manager_.SetNextXid(xid);
            }

            oid_t oid = GetRecordInfo(record).first;
            pageid_t page_id = GetRecordInfo(record).second;

            if (record->GetType() == LogType::INSERT || record->GetType() == LogType::DELETE || record->GetType() == LogType::NEW_PAGE) {
                // 更新脏页表
                if (dpt_.find({oid, page_id}) == dpt_.end()) {
                    dpt_[{oid, page_id}] = lsn;
                }
            }

            lsn += record->GetSize();
        }

        delete record_data;
    }

    void LogManager::Redo() {
        // 正序读取日志，调用日志记录的 Redo 函数
        // LAB 2 BEGIN
        lsn_t lsn = min_rec_lsn_;
        for (auto iter : dpt_) {
            lsn_t recLSN = iter.second;
            if (recLSN < lsn) {
                lsn = recLSN;
            }
        }

        char *record_data = static_cast<char*>(malloc(MAX_LOG_SIZE * sizeof(char)));

        while (lsn < next_lsn_) {
            disk_.ReadLog(lsn, MAX_LOG_SIZE, record_data);
            auto record = LogRecord::DeserializeFrom(lsn, record_data);

            oid_t oid = GetRecordInfo(record).first;
            pageid_t page_id = GetRecordInfo(record).second;

            if (record->GetType() == LogType::INSERT || record->GetType() == LogType::DELETE || record->GetType() == LogType::NEW_PAGE) {
                // 在脏页表
                if (dpt_.find({oid, page_id}) != dpt_.end()) {
                    lsn_t recLSN = dpt_[{oid, page_id}];
                    // 数据修改未在磁盘中生效
                    if (lsn >= recLSN) {
                        if (record->GetType() == LogType::NEW_PAGE) {
                            record->Redo(*buffer_pool_, *catalog_, *this);
                        } else {
                            oid_t db_oid = catalog_->GetDatabaseOid(oid);
                            auto current_page = buffer_pool_->GetPage(db_oid, oid, page_id);
                            TablePage table_page(current_page);
                            lsn_t pageLSN = table_page.GetPageLSN();

                            // 页面未在检查点后刷新
                            if (lsn > pageLSN) {
                                record->Redo(*buffer_pool_, *catalog_, *this);
                            }
                        }
                    }
                }
            }
            lsn += record->GetSize();
        }

        delete record_data;
    }

    void LogManager::Undo() {
        // 根据活跃事务表，将所有活跃事务回滚
        // LAB 2 BEGIN
        for (auto iter : att_) {
            Rollback(iter.first);
        }
    }

    std::pair<oid_t, pageid_t> LogManager::GetRecordInfo(std::shared_ptr<LogRecord>& record) {
        oid_t oid = 0;
        pageid_t page_id = 0;

        if (record->GetType() == LogType::INSERT) {
            auto insert_record = std::dynamic_pointer_cast<InsertLog>(record);
            assert(insert_record != nullptr);
            page_id = insert_record->GetPageId();
            oid = insert_record->GetOid();
        }
        else if (record->GetType() == LogType::DELETE) {
            auto delete_record = std::dynamic_pointer_cast<DeleteLog>(record);
            assert(delete_record != nullptr);
            page_id = delete_record->GetPageId();
            oid = delete_record->GetOid();
        }
        else if (record->GetType() == LogType::NEW_PAGE) {
            auto new_page_record = std::dynamic_pointer_cast<NewPageLog>(record);
            assert(new_page_record != nullptr);
            page_id = new_page_record->GetPageId();
            oid = new_page_record->GetOid();
        }
        else {}

        return std::make_pair(oid, page_id);
    }


}  // namespace huadb
