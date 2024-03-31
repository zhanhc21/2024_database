#pragma once

#include "storage/buffer_strategy.h"
#include <list>

namespace huadb {

    class LRUBufferStrategy : public BufferStrategy {
    public:
        void Access(size_t frame_no) override;
        size_t Evict() override;
    private:
        std::list<size_t> lru_buffer_;
    };

}  // namespace huadb
