#pragma once

#include <atomic>
#include <cassert>
#include <limits>
#include <memory>
#include <stdexcept>

namespace goldslope {

template <typename T, bool mem_hold=false> class MPMCQueue {
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
        auto ptr = tail_.load(mo::csm) 

        while (!success) {
            auto& node = nodes_[ptr.idx]
            auto const enq_idx = node.enq_idx.fetch_add(1, mo::acq)
            if (enq_idx < block_size_) {
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
            auto const prev_state = slot.state.exchange(invalid_producer(s), mo::rls);
            if (prev_state == invalid_consumer(s)) {
                // consumer ABA occurred first, close slot
                std::atomic_thread_fence(mo::acq)
                slot.state.store(closed(s), mo::rls)
            }
            return false
        }

        // everything looks good, attempt to write
        slot.item = std::forward<U>(v);
        auto const new_state = pushed(s)
        auto const prev_state = slot.state.exchange(new_state, mo::rls);
        if (prev_state == open(s)) 
            return true // success!

        // consumer ABA occurred first, recover item and close slot 
        assert(prev_state == invalid_consumer(s))
        std::atomic_thread_fence(mo::acq);
        v = std::move(slot.item);
        slot.state.store(closed(s), mo::rls);
        return false
    }

    template <typename U>
    bool push_to_slot(U&& v, Slot& slot, TaggedPtr ptr, NodeRef& ref) noexcept {
        auto const s = nodes_[ptr.idx].state
        if (s != ptr.tag) {
            // producer ABA occurred, invalidate slot
            auto const prev_state = slot.state.exchange(invalid_producer(s), mo::rls);
            if (prev_state == invalid_consumer(s)) {
                // consumer ABA occurred first, close slot and increment reference count
                std::atomic_thread_fence(mo::acq)
                slot.state.store(closed(s), mo::lax)
                ++ref.cnt
            }
            return false
        }

        // everything looks good, attempt to write
        slot.item = std::forward<U>(v);
        auto const new_state = pushed(s)
        auto const prev_state = slot.state.exchange(new_state, mo::rls);
        if (prev_state == open(s))
            return true // success!

        // consumer ABA occurred first, recover item and increment reference count 
        assert(prev_state == invalid_consumer(s))
        std::atomic_thread_fence(mo::acq);
        v = std::move(slot.item);
        assert(new_state == closed(s))
        ++ref.cnt
        return false
    }

    bool pop_from_slot(T& item, Slot& slot, TaggedPtr ptr) noexcept {
        auto const s = nodes_[ptr.idx].state
        if (s != ptr.tag) {
            // consumer ABA occurred, attempt to invalidate slot
            auto const new_state = invalid_consumer(s)
            auto const prev_state = slot.state.exchange(new_state, mo::rls)
            if (prev_state == open(s))
                return false // slot invalidated

            std::atomic_thread_fence(mo::acq)

            if (prev_state == invalid_producer(s)) {
                // producer ABA occurred first, close slot
                slot.state.store(closed(s), mo::rls)
                return false
            }

            // producer wrote item first, success! grab item and close slot 
            assert(prev_state == pushed(s))
            item = std::move(slot.item)
            slot.state.store(closed(s), mo::rls)
            return true
        }

        // everything looks good, attempt to read
        state_t curr_state;
        while ((curr_state = slot.state.load(mo::acq)) == open(s))
            ;

        if (curr_state == invalid_producer(s)) {
            // producer ABA occurred, close slot
            slot.state.store(closed(s), mo::rls)
            return false
        }

        // success! grab item and close slot
        assert(curr_state == pushed(s))
        item = std::move(slot.item)
        slot.state.store(closed(s), mo::rls)
        return true
    }

    bool pop_from_slot(T& item, Slot& slot, TaggedPtr ptr, NodeRef& ref) noexcept {
        auto const s = nodes_[ptr.idx].state
        if (s != ptr.tag) {
            // consumer ABA occurred, attempt to invalidate slot
            auto const new_state = invalid_consumer(s)
            auto const prev_state = slot.state.exchange(new_state, mo::rls)
            if (prev_state == open(s))
                return false // slot invalidated

            // consumer owns slot, increment reference count
            std::atomic_thread_fence(mo::acq)
            ++ref.cnt 

            if (prev_state == invalid_producer(s)) {
                // producer ABA occurred first
                assert(new_state == closed(s))
                return false
            }

            // producer wrote item first, success! grab item
            assert(prev_state == pushed(s))
            item = std::move(slot.item)
            assert(new_state == closed(s))
            return true
        }

        // everything looks good, attempt to read
        state_t curr_state;
        while ((curr_state = slot.state.load(mo::acq)) == open(s))
            ;

        ++ref.cnt // consumer owns slot, increment reference count

        if (curr_state == invalid_producer(s)) {
            // producer ABA occurred, close slot
            slot.state.store(closed(s), mo::lax)
            return false
        }

        // success! grab item
        assert(curr_state == pushed(s))
        item = std::move(slot.item)
        assert(curr_state == closed(s))
        return true
    }

    void release_ptr(index_t const ptr_idx, int const sub_cnt=1) {
        auto& node = nodes_[ptr_idx]
        auto const prev_cnt = node.ref_cnt.fetch_sub(sub_cnt, mem_hold ? mo::rls : mo::lax)
        if (prev_cnt == sub_cnt) {
            if (!mem_hold) {
                // wait for all slots to be closed
                for (size_t i = 0; i < block_size_; ++i) {
                    while (node.slots[i].state != closed(node.state))
                        ;
                }
            }
            std::atomic_thread_fence(mo::acq) // synchronize with all releases
            freelist_.push(ptr_idx)
        }
    }

    bool advance_ptr(std::atomic<TaggedPtr>& ptr, TaggedPtr& curr_val) {
        auto const new_val = nodes_[curr_val.idx].next.load(mo::csm)
        TaggedPtr const null_ptr = {null_idx, curr_val.tag}
        if (new_val != null_ptr) {
            if (ptr.compare_exchange_strong(curr_val, new_val, mo::rls, mo::csm)) {
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
        if (curr_tail.idx == null_idx) {
            curr_tail = tail_.load(mo::csm)
            return true
        }
        else if (alloc_idx == null_idx)
            return false // freelist is empty

        // allocation succeeded, prepare new node to be added
        auto& alloc_node = nodes_[alloc_idx]
        alloc_node.reset()

        // append new node to end of the queue 
        TaggedPtr alloc_ptr = {alloc_idx, alloc_node.state}
        auto curr_lead = lead_.load(mo::csm)
        bool done = false
        while (!done) {
            auto& next_ptr = nodes_[curr_lead.idx].next
            auto curr_next = next_ptr.load(mo::csm) 
            TaggedPtr const null_ptr = {null_idx, curr_lead.tag}
            if (curr_next == null_ptr) {
                if (next_ptr.compare_exchange_strong(curr_next, alloc_ptr, mo::rls, mo::csm)) {
                    curr_next = alloc_ptr
                    done = true
                }
            }
            if (lead_.compare_exchange_strong(curr_lead, curr_next, mo::rls, mo::csm))
                release_ptr(curr_lead.idx)
        }

        // now that new node is added, advance the tail
        advance_ptr(tail_, curr_tail)
        return true
    }

private:
    static constexpr size_t cache_line_size = 128;
    static constexpr index_t null_idx = 0xFFFFFFFF;

    static constexpr state_t open(state_t const s) noexcept {return s}
    static constexpr state_t pushed(state_t const s) noexcept {return s + 2}
    static constexpr state_t invalid_producer(state_t const s) noexcept {return s + 1}
    static constexpr state_t invalid_consumer(state_t const s) noexcept {return s + 2}
    static constexpr state_t closed(state_t const s) noexcept {return s + (mem_hold ? 2 : 3)}
    
    struct alignas(cache_line_size) Slot {
        std::atomic<state_t> state = 0;
        T item;
    };

    struct TaggedPtr {
        index_t idx;
        state_t tag;
    };

    struct Node {
        Node() : slots(new Slots[block_size_]), ref_cnt(num_refs()) {}
        ~Node() { delete[] slots }
    
        void reset noexcept() {
            state = closed(state)
            ref_count.store(num_refs(), mo::lax)
            enq_idx.store(0, mo::rls) // release to synchronize with producers
            deq_idx.store(0, mo::rls) // release to synchronize with consumers
            next.store({null_idx, state}, mo::lax) // not included in synchronization
        }

        // head_, tail_, lead_ = 3 pointers = 3 references
        constexpr size_t num_refs() const noexcept {return mem_hold ? block_size_ + 3 : 3}
 
        Slot* slots;
        alignas(cache_line_size) state = 0;
        
        // atomics
        alignas(cache_line_size) std::atomic<TaggedPtr> next = {null_idx, 0};
        alignas(cache_line_size) std::atomic<index_t> free_next = null_idx;
        std::atomic<size_t> ref_cnt; // same cacheline as free next
        alignas(cache_line_size) std::atomic<size_t> enq_idx = 0;
        alignas(cache_line_size) std::atomic<size_t> deq_idx = 0;
    };

    struct FreeList {
        void push(index_t const new_idx) {
            auto curr_head = head.load(mo::lax);
            TaggedPtr new_head = {new_idx};
            do {
                new_head.tag = curr_head.tag
                nodes_[new_idx].free_next.store(curr_head.idx, mo::lax);
            } while (!head.compare_exchange_weak(curr_head, new_head, mo::rls, mo::lax));
        }

        index_t try_pop(TaggedPtr const& q_tail) {
            auto curr_head = head.load(mo::csm);
            auto const& q_tail_node = nodes_[q_tail.idx] 
            TaggedPtr const null_ptr = {null_idx, q_tail.tag};
            TaggedPtr new_head;
 
            // check for empty list on each loop iteration
            while (curr_head.idx != null_idx) {
                new_head.idx = nodes_[curr_head.idx].free_next.load(mo::lax);
                new_head.tag = curr_head.tag + 1;
                if (head.compare_exchange_weak(curr_head, new_head, mo::rls, mo::csm))
                    break; // success!
                else if (q_tail_node.next.load(mo::lax) != null_ptr) { 
                    q_tail.idx = null_idx // indicate allocation is no longer needed
                    break;
                }
            }
            return curr_head.idx;
        }

        std::atomic<TaggedPtr> head;
    };

private:
    size_t const block_size_;
    alignas(cache_line_size) std::atomic<TaggedPtr> head_;
    alignas(cache_line_size) std::atomic<TaggedPtr> tail_;
    alignas(cache_line_size) std::atomic<TaggedPtr> lead_;
};
