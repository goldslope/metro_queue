#pragma once

#include <atomic>
#include <cassert>
#include <limits>
#include <memory>
#include <stdexcept>

namespace goldslope {

template <typename T, bool DEFERRED_RELEASE=false> class MPMCQueue {
public:
    explicit MPMCQueue(size_t const capacity) {
    }

    ~MPMCQueue() noexcept {
    }

    // non-copyable and non-movable
    MPMCQueue(MPMCQueue const&) = delete;
    MPMCQueue& operator=(MPMCQueue const&) = delete;

    void push(T&& v) noexcept {
    }

    bool try_push(T&& v) noexcept {
        auto success = false; 
        auto ptr = tail_.load(C_idx) 

        while (!success) {
            auto& node = nodes_[ptr.idx]
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

    void pop(T& v) noexcept {
    }

    bool try_pop(T& v) noexcept {
    }

private
    template <typename U>
    bool push_to_slot(U&& v, Slot& slot, TaggedPtr ptr) noexcept {
        auto const s = nodes_[ptr.idx].state
        if (s != ptr.tag) {
            // producer ABA occurred, invalidate slot
            auto const prev_state = slot.state.exchange(INVALID_PRODUCER(s), R);
            if (prev_state == INVALID_CONSUMER(s)) {
                // consumer ABA occurred first, close slot
                std::atomic_thread_fence(A)
                slot.state.store(CLOSED(s), R)
            }
            return false
        }

        // everything looks good, attempt to write
        slot.item = std::forward<U>(v);
        auto const new_state = PUSHED(s)
        auto const prev_state = slot.state.exchange(new_state, R);
        if (prev_state == OPEN(s)) 
            return true // success!

        // consumer ABA occurred first, recover item and close slot 
        assert(prev_state == INVALID_CONSUMER(s))
        std::atomic_thread_fence(A);
        v = std::move(slot.item);
        slot.state.store(CLOSED(s), R);
        return false
    }

    template <typename U>
    bool push_to_slot(U&& v, Slot& slot, TaggedPtr ptr, NodeRef& ref) noexcept {
        auto const s = nodes_[ptr.idx].state
        if (s != ptr.tag) {
            // producer ABA occurred, invalidate slot
            auto const prev_state = slot.state.exchange(INVALID_PRODUCER(s), R);
            if (prev_state == INVALID_CONSUMER(s)) {
                // consumer ABA occurred first, close slot and increment reference count
                std::atomic_thread_fence(A)
                slot.state.store(CLOSED(s), X)
                ++ref.cnt
            }
            return false
        }

        // everything looks good, attempt to write
        slot.item = std::forward<U>(v);
        auto const new_state = PUSHED(s)
        auto const prev_state = slot.state.exchange(new_state, R);
        if (prev_state == OPEN(s))
            return true // success!

        // consumer ABA occurred first, recover item and increment reference count 
        assert(prev_state == INVALID_CONSUMER(s))
        std::atomic_thread_fence(A);
        v = std::move(slot.item);
        assert(new_state == CLOSED(s))
        ++ref.cnt
        return false
    }

    bool pop_from_slot(T& item, Slot& slot, TaggedPtr ptr) noexcept {
        auto const s = nodes_[ptr.idx].state
        if (s != ptr.tag) {
            // consumer ABA occurred, attempt to invalidate slot
            auto const new_state = INVALID_CONSUMER(s)
            auto const prev_state = slot.state.exchange(new_state, R)
            if (prev_state == OPEN(s))
                return false // slot invalidated

            std::atomic_thread_fence(A)

            if (prev_state == INVALID_PRODUCER(s)) {
                // producer ABA occurred first, close slot
                slot.state.store(CLOSED(s), R)
                return false
            }

            // producer wrote item first, success! grab item and close slot 
            assert(prev_state == PUSHED(s))
            item = std::move(slot.item)
            slot.state.store(CLOSED(s), R)
            return true
        }

        // everything looks good, attempt to read
        uint32_t curr_state;
        while ((curr_state = slot.state.load(A)) == OPEN(s))
            ;

        if (curr_state == INVALID_PRODUCER(s)) {
            // producer ABA occurred, close slot
            slot.state.store(CLOSED(s), R)
            return false
        }

        // success! grab item and close slot
        assert(curr_state == PUSHED(s))
        item = std::move(slot.item)
        slot.state.store(CLOSED(s), R)
        return true
    }

    bool pop_from_slot(T& item, Slot& slot, TaggedPtr ptr, NodeRef& ref) noexcept {
        auto const s = nodes_[ptr.idx].state
        if (s != ptr.tag) {
            // consumer ABA occurred, attempt to invalidate slot
            auto const new_state = INVALID_CONSUMER(s)
            auto const prev_state = slot.state.exchange(new_state, R)
            if (prev_state == OPEN(s))
                return false // slot invalidated

            // consumer owns slot, increment reference count
            std::atomic_thread_fence(A)
            ++ref.cnt 

            if (prev_state == INVALID_PRODUCER(s)) {
                // producer ABA occurred first
                assert(new_state == CLOSED(s))
                return false
            }

            // producer wrote item first, success! grab item
            assert(prev_state == PUSHED(s))
            item = std::move(slot.item)
            assert(new_state == CLOSED(s))
            return true
        }

        // everything looks good, attempt to read
        uint32_t curr_state;
        while ((curr_state = slot.state.load(A)) == OPEN(s))
            ;

        ++ref.cnt // consumer owns slot, increment reference count

        if (curr_state == INVALID_PRODUCER(s)) {
            // producer ABA occurred, close slot
            slot.state.store(CLOSED(s), X)
            return false
        }

        // success! grab item
        assert(curr_state == PUSHED(s))
        item = std::move(slot.item)
        assert(curr_state == CLOSED(s))
        return true
    }

    void release_ptr(uint32_t const ptr_idx, int const decrement=1) {
        auto& node = nodes_[ptr_idx]
        auto const cnt = node.ref_cnt.fetch_sub(decrement, DEFERRED_RELEASE ? R : X) 
        if (cnt == decrement) {
            if (!DEFERRED_RELEASE) {
                // wait for all slots to be closed
                for (size_t i = 0; i < blocksize_; ++i) {
                    while (node.slots[i].state != CLOSED(node.state))
                        ;
                }
            }
            std::atomic_thread_fence(A) // synchronize with all releases
            freelist_.push(ptr_idx)
        }
    }

    bool advance_ptr(std::atomic<TaggedPtr>& ptr, TaggedPtr& curr_val) {
        auto const new_val = nodes_[curr_val.idx].next.load(C_atm)
        TaggedPtr const null_ptr = {kNull, curr_val.tag}
        if (new_val != null_ptr) {
            if (ptr.compare_exchange_strong(curr_val, new_val, R, C_idx)) {
                release_ptr(curr_val.idx)
                curr_val = new_val
            }
            return true
        }
 
        return false
    }

    bool get_next_tail(TaggedPtr& curr_tail) noexcept {
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
        auto& alloc_node = nodes_[alloc_idx]
        alloc_node.reset()

        // append new node to end of the queue 
        TaggedPtr alloc_ptr = {alloc_idx, alloc_node.state}
        auto curr_lead = lead_.load(C_idx)
        bool done = false
        while (!done) {
            auto& next_ptr = nodes_[curr_lead.idx].next
            auto curr_next = next_ptr.load(C_atm) 
            TaggedPtr const null_ptr = {kNull, curr_lead.tag}
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

    static constexpr uint32_t OPEN(uint32_t const s) noexcept {return s}
    static constexpr uint32_t PUSHED(uint32_t const s) noexcept {return s + 2}
    static constexpr uint32_t INVALID_PRODUCER(uint32_t const s) noexcept {return s + 1}
    static constexpr uint32_t INVALID_CONSUMER(uint32_t const s) noexcept {return s + 2}
    static constexpr uint32_t CLOSED(uint32_t const s) noexcept {return s + (DEFERRED_RELEASE ? 2 : 3)}
    
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
            state = CLOSED(state)
            ref_count.store(num_references(), X)
            enq_idx.store(0, R) // release to synchronize with producers
            deq_idx.store(0, R) // release to synchronize with consumers
            next.store({kNull, state}, X) // not included in synchronization
        }

        // head_, tail_, lead_ = 3 pointers = 3 references
        constexpr size_t num_references() const noexcept {DEFERRED_RELEASE ? blocksize_ + 3 : 3} 
        
        Slot* slots;
        alignas(kCacheLineSize) state = 0;
        
        // atomics
        alignas(kCacheLineSize) std::atomic<TaggedPtr> next = {kNull, 0};
        alignas(kCacheLineSize) std::atomic<uint32_t> free_next = kNull;
        std::atomic<size_t> ref_cnt; // same cacheline as free next
        alignas(kCacheLineSize) std::atomic<size_t> enq_idx = 0;
        alignas(kCacheLineSize) std::atomic<size_t> deq_idx = 0;
    };

    struct FreeList {
        void push(uint32_t const new_idx) {
            auto curr_head = head.load(X);
            TaggedPtr new_head = {new_idx};
            do {
                new_head.tag = curr_head.tag
                nodes_[new_idx].free_next.store(curr_head.idx, X);
            } while (!head.compare_exchange_weak(curr_head, new_head, R, X));
        }

        uint32_t try_pop(TaggedPtr const& q_tail) {
            auto curr_head = head.load(C_idx);
            auto const& q_tail_node = nodes_[q_tail.idx] 
            TaggedPtr const null_ptr = {kNull, q_tail.tag};
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
    size_t const blocksize_;
    alignas(kCacheLineSize) std::atomic<TaggedPtr> head_;
    alignas(kCacheLineSize) std::atomic<TaggedPtr> tail_;
    alignas(kCacheLineSize) std::atomic<TaggedPtr> lead_;
};
