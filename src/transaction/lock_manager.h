#pragma once

#include <vector>
#include "common/constants.h"
#include "common/types.h"

namespace huadb {

    enum class LockType {
        IS,   // 意向共享锁
        IX,   // 意向互斥锁
        S,    // 共享锁
        SIX,  // 共享意向互斥锁
        X,    // 互斥锁
    };

    enum class LockGranularity {
        TABLE, ROW
    };

// 高级功能：死锁预防/检测类型
    enum class DeadlockType {
        NONE, WAIT_DIE, WOUND_WAIT, DETECTION
    };

    typedef struct {
        LockType lock_type_;
        LockGranularity granularity_;
        xid_t xid_;
        Rid rid_;
    } Lock;

    class LockManager {
    public:
        // 获取表级锁
        bool LockTable(xid_t xid, LockType lock_type, oid_t oid);

        // 获取行级锁
        bool LockRow(xid_t xid, LockType lock_type, oid_t oid, Rid rid);

        // 释放事务申请的全部锁
        void ReleaseLocks(xid_t xid);

        void SetDeadLockType(DeadlockType deadlock_type);

        const std::vector<std::vector<bool>> compatible_matrix_ = {{true,  true,  true,  true,  false},
                                                                   {true,  true,  false, false, false},
                                                                   {true,  false, true,  false, false},
                                                                   {true,  false, false, false, false},
                                                                   {false, false, false, false, false}};

    private:
        // 判断锁的相容性
        bool Compatible(LockType type_a, LockType type_b);

        // 实现锁的升级，如共享锁升级为互斥锁，输入两种锁的类型，返回升级后的锁类型
        LockType Upgrade(LockType self, LockType other) const;

        DeadlockType deadlock_type_ = DeadlockType::NONE;

        std::unordered_map<oid_t, std::vector<Lock>> locks_;
    };

}  // namespace huadb
