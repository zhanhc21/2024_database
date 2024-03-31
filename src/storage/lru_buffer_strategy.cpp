#include "storage/lru_buffer_strategy.h"

namespace huadb {

    void LRUBufferStrategy::Access(size_t frame_no) {
        // 缓存页面访问
        // LAB 1 BEGIN
        lru_buffer_.remove(frame_no);
        lru_buffer_.emplace_front(frame_no);
    };

    size_t LRUBufferStrategy::Evict() {
        // 缓存页面淘汰，返回淘汰的页面在 buffer pool 中的下标
        // LAB 1 BEGIN
        auto last = lru_buffer_.back();
        lru_buffer_.pop_back();
        return last;
    }

}  // namespace huadb
