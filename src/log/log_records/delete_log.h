#pragma once

#include "log/log_record.h"

namespace huadb {

    class DeleteLog : public LogRecord {
    public:
        DeleteLog(lsn_t lsn, xid_t xid, lsn_t prev_lsn, oid_t oid, pageid_t page_id, slotid_t slot_id);

        size_t SerializeTo(char *data) const override;

        static std::shared_ptr<DeleteLog> DeserializeFrom(lsn_t lsn, const char *data);

        void Undo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager, lsn_t undo_next_lsn) override;

        void Redo(BufferPool &buffer_pool, Catalog &catalog, LogManager &log_manager) override;

        oid_t GetOid() const;

        pageid_t GetPageId() const;

        std::string ToString() const override;

    private:
        oid_t oid_;
        pageid_t page_id_;
        slotid_t slot_id_;
    };

}  // namespace huadb
