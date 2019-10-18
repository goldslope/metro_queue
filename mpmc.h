#pragma once

#include <atomic>
#include <cassert>
#include <limits>
#include <memory>
#include <stdexcept>

namespace goldslope {

template <typename T> class MPMCQueue {
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
        auto ptr = tail_.load(A) 

        while (!success) {
            auto &node = nodes[ptr.idx]
            auto const enq_idx = node.enq_idx.fetch_add(1, A)
            if (enq_idx < blocksize_) {
                // there is room for us, try pushing to slot
                success = push_to_slot(
                        std::forward<T>(v),
                        node.slots[enq_idx],
                        node,
                        ptr.tag);
            } else {
                auto const curr_next = node.next.load(X)
                if (curr_next.idx != kNull)
                    divider_.compare_exchange_strong(ptr, curr_next, R, A);
                else if (!get_next_tail(ptr))
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
    bool push_to_slot(
            U &&v,
            Slot& slot,
            Node& node,
            uint32_t ptr_state) noexcept {

        if (node.state == ptr_state) {
            // everything looks good, attempt to write
            slot.item = std::forward<U>(v);
            auto const prev_state = slot.state.exchange(node.state + 1, R);
            if (prev_state == node.state) 
                return true // success!

            // consumer ABA occurred, recover item and close state 
            v = std::move(slot.item);
            slot.state.store(node.state + 2, R);
        } else {
            // producer ABA occurred, invalidate slot
            slot.state.exchange(node.state + 2, R);
        }

        return false
    }

    bool get_next_tail(TaggedPtr &curr_tail) noexcept {
        // first try to advance tail
        auto const new_tail = nodes[curr_tail.idx].next.load(A) 
        TaggedPtr null_ptr = {kNull, curr_tail.tag}
        if (new_tail != null_ptr) {
            tail_.compare_exchange_strong(curr_tail, new_tail, R, A)
            return true
        }
 
        auto const alloc_idx = freelist_.try_pop(curr_tail);
        if (curr_tail.idx == kNull) {
            curr_tail = tail_.load(A)
            return true
        }
        else if (alloc_idx == kNull)
            return false // freelist is empty


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
        Node() : slots(new Slots[blocksize_]) {}
        ~Node() { delete[] slots }
        
        Slot *slots;
        alignas(kCacheLineSize) state = 0;
        
        // atomics
        alignas(kCacheLineSize) std::atomic<TaggedPtr> next = {0, 0};
        alignas(kCacheLineSize) std::atomic<uint32_t> free_next = 0;
        std::atomic<size_t> ref_cnt = 0; // same cacheline as free next
        alignas(kCacheLineSize) std::atomic<size_t> enq_idx = 0;
        alignas(kCacheLineSize) std::atomic<size_t> deq_idx = 0;
    };

    struct FreeList {
        void push(uint32_t new_idx) {
            auto const curr_head = head.load(X);
            TaggedPtr new_head = {new_idx};
            do {
                new_head.tag.store(curr_head.tag, X)
                nodes[new_idx].free_next.store(curr_head.idx, X);
            } while (!head.compare_exchange_weak(curr_head, new_head, R, X));
        }

        uint32_t try_pop(TaggedPtr &q_tail) {
            auto const curr_head = head.load(A);
            auto const &q_tail_node = nodes[q_tail.idx] 
            TaggedPtr null_ptr = {kNull, q_tail.tag};
            TaggedPtr new_head;
           
            // check for empty list on each loop iteration
            while (curr_head.idx != kNull) {
                new_head.idx = nodes[curr_head.idx].free_next.load(X);
                new_head.tag = curr_head.tag + 1;
                if (head.compare_exchange_weak(curr_head, new_head, R, A))
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
