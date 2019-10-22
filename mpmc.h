#pragma once

#include <atomic>
#include <cassert>
#include <limits>
#include <memory>
#include <stdexcept>

namespace goldslope {

template <typename T, bool DEFERRED_RELEASE=false> class MPMCQueue {
public:
    explicit MPMCQueue(const size_t capacity) {
    }

    ~MPMCQueue() noexcept {
    }

    // non-copyable and non-movable
    MPMCQueue(const MPMCQueue &) = delete;
    MPMCQueue &operator=(const MPMCQueue &) = delete;

    void push(T &&v) noexcept {
    }

    bool try_push(T &&v) noexcept {
        auto success = false; 
        auto ptr = tail_.load(C) 

        while (!success) {
            auto &node = nodes_[ptr.idx]
            auto const enq_idx = node.enq_idx.fetch_add(1, A)
            if (enq_idx < blocksize_) {
                // there is room for us, try pushing to slot
                success = push_to_slot(std::forward<T>(v), node.slots[enq_idx], ptr);
            } else {
                if (!get_next_tail(ptr))
                    return false
            }
        }
    }

    void pop(T &v) noexcept {
    }

    bool try_pop(T &v) noexcept {
    }

private
    template <typename U>
    bool push_to_slot(U &&v, Slot& slot, TaggedPtr ptr) noexcept {
        auto &node = nodes_[ptr.idx]
        if (node.state == ptr.tag) {
            // everything looks good, attempt to write
            slot.item = std::forward<U>(v);
            auto const prev_state = slot.state.exchange(node.state + 1, R);
            if (prev_state == node.state) 
                return true // success!

            // consumer ABA occurred, recover item and close slot 
            v = std::move(slot.item);
            slot.state.store(node.state + 2, R);
        } else {
            // producer ABA occurred, close slot
            slot.state.exchange(node.state + 2, R);
        }

        return false
    }

    void release_ptr(uint32_t ptr_idx, int decrement=1) {
        auto &node = nodes_[ptr_idx]
        auto const cnt = node.ref_cnt.fetch_sub(decrement, DEFERRED_RELEASE ? R : X) 
        if (cnt == decrement) {
            if (DEFERRED_RELEASE == false) {
                // wait for all slots to be closed
                for (size_t i = 0; i < blocksize_; ++i) {
                    while (node.slots[i].state != node.state + 2)
                        ;
                }
            }
            std::atomic_thread_fence(A) // synchronize with all releases
            freelist_.push(ptr_idx)
        }
    }

    bool advance_ptr(std::atomic<TaggedPtr> &ptr, TaggedPtr &curr_val) {
        auto const new_val = nodes_[curr_val.idx].next.load(C_atm)
        TaggedPtr null_ptr = {kNull, curr_val.tag}
        if (new_val != null_ptr) {
            if (ptr.compare_exchange_strong(curr_val, new_val, R, C_idx)) {
                release_ptr(curr_val.idx)
                curr_val = new_val
            }
            return true
        }
 
        return false
    }

    bool get_next_tail(TaggedPtr &curr_tail) noexcept {
        // attempt to advance the tail first
        if (advance_ptr(tail_, curr_tail))
            return true

        // no more nodes in the queue, attempt to allocate
        auto const alloc_idx = freelist_.try_pop(curr_tail);
        if (curr_tail.idx == kNull) {
            curr_tail = tail_.load(C_idx)
            return true
        }
        else if (alloc_idx == kNull)
            return false // freelist is empty

        // allocation succeeded, prepare new node to be added
        auto &alloc_node = nodes_[alloc_idx]
        alloc_node.reset()

        // append new node to end of the queue 
        TaggedPtr alloc_ptr = {alloc_idx, alloc_node.state}
        auto curr_lead = lead_.load(C_idx)
        bool done = false
        while (!done) {
            auto &next_ptr = nodes_[curr_lead.idx].next
            auto curr_next = next_ptr.load(C_atm) 
            TaggedPtr null_ptr = {kNull, curr_lead.tag}
            if (curr_next == null_ptr) {
                if (next_ptr.compare_exchange_strong(curr_next, alloc_ptr, R, C_atm)) {
                    curr_next = alloc_ptr
                    done = true
                }
            }
            if (lead_.compare_exchange_strong(curr_lead, curr_next, R, C_idx))
                release_ptr(curr_lead.idx)
        }

        // now that new node is added, advance the tail
        advance_ptr(tail_, curr_tail)
        return true
    }

private:
    static constexpr size_t kCacheLineSize = 128;
    static constexpr uint32_t kNull = 0xFFFFFFFF;

    struct alignas(kCacheLineSize) Slot {
        std::atomic<uint32_t> state = 0;
        T item;
    };

    struct TaggedPtr {
        uint32_t idx;
        uint32_t tag;
    };

    struct Node {
        Node() : slots(new Slots[blocksize_]), ref_cnt(num_references()) {}
        ~Node() { delete[] slots }
    
        void reset noexcept() {
            state += 2 
            ref_count.store(num_references(), X)
            enq_idx.store(0, R) // release to synchronize with producers
            deq_idx.store(0, R) // release to synchronize with consumers
            next.store({kNull, state}, X) // not included in synchronization
        }

        // head_, tail_, lead_ = 3 pointers = 3 references
        constexpr size_t num_references() const noexcept {DEFERRED_RELEASE ? blocksize_ + 3 : 3} 
        
        Slot *slots;
        alignas(kCacheLineSize) state = 0;
        
        // atomics
        alignas(kCacheLineSize) std::atomic<TaggedPtr> next = {kNull, 0};
        alignas(kCacheLineSize) std::atomic<uint32_t> free_next = kNull;
        std::atomic<ssize_t> ref_cnt; // same cacheline as free next
        alignas(kCacheLineSize) std::atomic<size_t> enq_idx = 0;
        alignas(kCacheLineSize) std::atomic<size_t> deq_idx = 0;
    };

    struct FreeList {
        void push(uint32_t new_idx) {
            auto curr_head = head.load(X);
            TaggedPtr new_head = {new_idx};
            do {
                new_head.tag = curr_head.tag
                nodes_[new_idx].free_next.store(curr_head.idx, X);
            } while (!head.compare_exchange_weak(curr_head, new_head, R, X));
        }

        uint32_t try_pop(TaggedPtr &q_tail) {
            auto curr_head = head.load(C_idx);
            auto const &q_tail_node = nodes_[q_tail.idx] 
            TaggedPtr null_ptr = {kNull, q_tail.tag};
            TaggedPtr new_head;
 
            // check for empty list on each loop iteration
            while (curr_head.idx != kNull) {
                new_head.idx = nodes_[curr_head.idx].free_next.load(X);
                new_head.tag = curr_head.tag + 1;
                if (head.compare_exchange_weak(curr_head, new_head, R, C_idx))
                    break; // success!
                else if (q_tail_node.next.load(X) != null_ptr) { 
                    q_tail.idx = kNull // indicate allocation is no longer needed
                    break;
                }
            }
            return curr_head.idx;
        }

        std::atomic<TaggedPtr> head;
    };

private:
    size_t blocksize_; 
    alignas(kCacheLineSize) std::atomic<TaggedPtr> head_;
    alignas(kCacheLineSize) std::atomic<TaggedPtr> tail_;
    alignas(kCacheLineSize) std::atomic<TaggedPtr> lead_;
};
