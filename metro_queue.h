#pragma once

#include <algorithm>
#include <atomic>
#include <memory>
#include <new>
#include <type_traits>


namespace metro {

using std::size_t;

template <typename,
          typename,
          bool single_producer,
          bool single_consumer,
          bool node_ref_ok,
          typename = std::enable_if_t<!single_consumer || !node_ref_ok>>
class Queue;

template <typename T,
          typename Alloc,
          bool single_producer,
          bool single_consumer,
          bool node_ref_ok>
class QueuePtr {

    using QueueType = Queue<T, Alloc, single_producer, single_consumer, node_ref_ok>;

private:
    struct Key {
        explicit Key(int) noexcept {}
    };

public:
    QueuePtr(size_t queue_capacity, size_t node_capacity, Alloc const& alloc = Alloc{})
        : q_ptr{std::allocate_shared<QueueType>(alloc,
                                                Key{0},
                                                queue_capacity,
                                                node_capacity,
                                                alloc)} {}

    std::shared_ptr<QueueType> const& operator->() const noexcept {return q_ptr;}

    bool operator==(QueuePtr const& other) const noexcept {return q_ptr == other.q_ptr;}
    bool operator!=(QueuePtr const& other) const noexcept {return q_ptr != other.q_ptr;}

private:
    QueueType* const get() const noexcept {return q_ptr.get();}

    // disable heap allocation
    void* operator new(size_t) = delete;
    void* operator new[](size_t) = delete;
    void operator delete(void*) = delete;
    void operator delete[](void*) = delete;

    std::shared_ptr<QueueType> const q_ptr; // single const member -> immutable

    friend class Queue<T, Alloc, single_producer, single_consumer, node_ref_ok>;
};

template <typename T, typename Alloc = std::allocator<void>>
using MPMCQueuePtr = QueuePtr<T, Alloc, false, false, false>;

template <typename T, typename Alloc = std::allocator<void>>
using SPMCQueuePtr = QueuePtr<T, Alloc, true, false, false>;

template <typename T, typename Alloc = std::allocator<void>>
using MPSCQueuePtr = QueuePtr<T, Alloc, false, true, false>;

template <typename T, typename Alloc = std::allocator<void>>
using SPSCQueuePtr = QueuePtr<T, Alloc, true, true, false>;

template <typename T, typename Alloc = std::allocator<void>>
using MPMCRefQueuePtr = QueuePtr<T, Alloc, false, false, true>;

template <typename T, typename Alloc = std::allocator<void>>
using SPMCRefQueuePtr = QueuePtr<T, Alloc, true, false, true>;

template <typename T,
          typename Alloc,
          bool single_producer,
          bool single_consumer,
          bool node_ref_ok,
          typename>
class Queue {

    struct mo {
        static constexpr auto lax = std::memory_order_relaxed;
        static constexpr auto csm = std::memory_order_acquire; // consume = acquire, for now
        static constexpr auto acq = std::memory_order_acquire;
        static constexpr auto rls = std::memory_order_release;
    };

    using address_t = uint32_t;
    using state_t = uint32_t;
    using mo_t = std::memory_order;
    using QueuePtrType = QueuePtr<T, Alloc, single_producer, single_consumer, node_ref_ok>;

public:
    class NodeRef {
    public:
        explicit NodeRef(QueuePtrType const& q) noexcept : q_ptr{q}, addr{null_addr}, cnt{0} {}
        ~NodeRef() {release();}

        void release() noexcept {
            if (cnt > 0 && addr != null_addr) {
                q_ptr->remove_node_reference<mo::rls>(addr, cnt);
                cnt = 0;
            }
        }

        // disable copy and move constructor
        NodeRef(NodeRef const&) = delete;

        // disable heap allocation
        void* operator new(size_t) = delete;
        void* operator new[](size_t) = delete;
        void operator delete(void*) = delete;
        void operator delete[](void*) = delete;

    private:
        QueuePtrType const q_ptr; // const member -> non-assignable
        address_t addr;
        size_t cnt;

        friend class Queue;
    };

private:
    template <bool enabled>
    struct TmpNodeRef {
        TmpNodeRef(Queue* const q) noexcept : q_ptr{q}, addr{null_addr}, cnt{0} {}
        TmpNodeRef(Queue* const q, address_t a) noexcept : q_ptr{q}, addr{a}, cnt{0} {}
        ~TmpNodeRef() {release();}

        void release() noexcept {
            if (enabled && cnt > 0 && addr != null_addr) {
                q_ptr->remove_node_reference<mo::rls>(addr, cnt);
                cnt = 0;
            }
        }

        Queue* const q_ptr;
        address_t addr;
        size_t cnt;
    };

    template <typename U>
    struct TmpIterator {
        constexpr TmpIterator(U&& v) noexcept : val{v} {}
        constexpr U&& operator*() const noexcept {return val;}
        constexpr void operator+=(int) const noexcept {}

        U&& val;
    };

    struct TaggedPtr;
    struct Node;
    struct StatelessSlot;
    struct StatefulSlot;

    using Slot = std::conditional_t<single_producer && single_consumer, StatelessSlot, StatefulSlot>;
    using NodeAlloc = typename std::allocator_traits<Alloc>::template rebind_alloc<Node>;
    using SlotAlloc = typename std::allocator_traits<Alloc>::template rebind_alloc<Slot>;

public:
    Queue(typename QueuePtrType::Key const&,
          size_t queue_capacity,
          size_t node_capacity,
          Alloc const& alloc)
        : node_alloc_{alloc}, slot_alloc_{alloc} {

        size_t const min_node_capacity = 1;
        node_capacity = std::max(node_capacity, min_node_capacity);

        // determine number of nodes
        size_t const min_num_nodes = 2;
        size_t const max_num_nodes = null_addr;
        num_nodes_ = round_divide(queue_capacity, node_capacity);
        num_nodes_ = std::max(num_nodes_, min_num_nodes);
        num_nodes_ = std::min(num_nodes_, max_num_nodes);

        // determine slots per node
        slots_per_node_ = round_divide(queue_capacity, num_nodes_);
        slots_per_node_ = std::max(slots_per_node_, node_capacity);

        // allocate and initialize nodes
        nodes_ = std::allocator_traits<NodeAlloc>::allocate(node_alloc_, num_nodes_);
        for (size_t i = 0; i < num_nodes_; ++i) {
            std::allocator_traits<NodeAlloc>::construct(node_alloc_, &nodes_[i], refs_per_node());
        }

        // allocate and initialize slots
        auto const num_slots = num_nodes_ * slots_per_node_;
        slots_ = std::allocator_traits<SlotAlloc>::allocate(slot_alloc_, num_slots);
        for (size_t i = 0; i < num_slots; ++i) {
            std::allocator_traits<SlotAlloc>::construct(slot_alloc_, &slots_[i]);
        }

        // setup free-list
        for (address_t i = 1; i < num_nodes_; ++i) {
            nodes_[i].next = {i - 1, 0};
        }
        free_list_.head = {static_cast<address_t>(num_nodes_ - 1), 0};

        // setup queue
        auto const init_addr = free_list_.try_alloc(nodes_);
        nodes_[init_addr].reset(refs_per_node());
        head_ = {init_addr, 0};
        tail_ = {init_addr, 0};
        back_ = {init_addr, 0};
    }

    ~Queue() {
        // destroy and deallocate nodes
        for (size_t i = 0; i < num_nodes_; ++i) {
            std::allocator_traits<NodeAlloc>::destroy(node_alloc_, &nodes_[i]);
        }
        std::allocator_traits<NodeAlloc>::deallocate(node_alloc_, nodes_, num_nodes_);

        // destroy and deallocate slots
        auto const num_slots = num_nodes_ * slots_per_node_;
        for (size_t i = 0; i < num_slots; ++i) {
            std::allocator_traits<SlotAlloc>::destroy(slot_alloc_, &slots_[i]);
        }
        std::allocator_traits<SlotAlloc>::deallocate(slot_alloc_, slots_, num_slots);
    }

private:
    template <typename U>
    static constexpr auto is_valid_pop_value =
        std::is_nothrow_assignable_v<U, T&> || std::is_nothrow_assignable_v<U, T&&>;

    template <typename U>
    static constexpr auto is_valid_push_value =
        std::is_nothrow_assignable_v<T&, U> && (single_consumer || is_valid_pop_value<U>);

    template <typename Iter>
    static constexpr auto is_valid_base_iterator =
        std::is_same_v<typename std::iterator_traits<Iter>::iterator_category,
                       std::random_access_iterator_tag>;

    template <typename Iter>
    static constexpr auto is_valid_push_iterator =
        is_valid_push_value<std::iterator_traits<Iter>::reference> &&
        is_valid_base_iterator<Iter>;

    template <typename Iter>
    static constexpr auto is_valid_pop_iterator =
        is_valid_pop_value<std::iterator_traits<Iter>::reference> &&
        is_valid_base_iterator<Iter>;

public:
    template <typename U, typename = std::enable_if_t<is_valid_push_value<U>>>
    bool try_push(U&& v) noexcept {
        if constexpr (!single_producer) {
            return try_push_mp(TmpIterator<U>{v}, 1);
        }

        return try_push_sp(v);
    }

    template <typename U, typename = std::enable_if_t<is_valid_pop_value<U>>>
    bool try_pop(U&& v) noexcept {
        if constexpr (!single_consumer) {
            return try_pop_mc(TmpIterator<U>{v}, 1, TmpNodeRef<node_ref_ok>{this});
        }

        return try_pop_sc(v);
    }

    template <typename U, typename = std::enable_if_t<node_ref_ok && is_valid_pop_value<U>>>
    bool try_pop(U&& v, NodeRef& ref) noexcept {
        if (ref.q_ptr.get() != this) {
            return try_pop_mc(TmpIterator<U>{v}, 1, TmpNodeRef<node_ref_ok>{this});
        }

        return try_pop_mc(TmpIterator<U>{v}, 1, ref);
    }

    template <typename Iter, typename = std::enable_if_t<!single_producer &&
                                                         is_valid_push_iterator<Iter>>>
    size_t try_push(Iter begin, Iter end) noexcept {
        auto const item_cnt = end - begin;
        if (item_cnt > 0) {
            return try_push_mp(begin, item_cnt);
        }

        return 0;
    }

    template <typename Iter, typename = std::enable_if_t<!single_consumer &&
                                                         is_valid_pop_iterator<Iter>>>
    size_t try_pop(Iter begin, Iter end) noexcept {
        auto const item_cnt = end - begin;
        if (item_cnt > 0) {
            return try_pop_mc(begin, item_cnt, TmpNodeRef<node_ref_ok>{this});
        }

        return 0;
    }

    template <typename Iter, typename = std::enable_if_t<node_ref_ok &&
                                                         is_valid_pop_iterator<Iter>>>
    size_t try_pop(Iter begin, Iter end, NodeRef& ref) noexcept {
        auto const item_cnt = end - begin;
        if (item_cnt > 0) {
            if (ref.q_ptr.get() != this) {
                return try_pop_mc(begin, item_cnt, TmpNodeRef<node_ref_ok>{this});
            }

            return try_pop_mc(begin, item_cnt, ref);
        }

        return 0;
    }

private:
#ifdef __cpp_lib_hardware_interference_size
    static constexpr auto interference_size = std::hardware_destructive_interference_size;
#else
    static constexpr auto interference_size = 64;
#endif
    static constexpr auto null_addr = std::numeric_limits<address_t>::max();
    static constexpr auto round_divide(size_t const a, size_t const b) noexcept {
        auto const max_a = std::numeric_limits<size_t>::max();
        auto const round_a = std::min(a, max_a - b + 1) + b - 1;
        return round_a / b;
    }

    static constexpr state_t open(state_t const s) noexcept {return s;}
    static constexpr state_t pushed(state_t const s) noexcept {
        return s + (single_producer ? 1 : 2);
    }
    static constexpr state_t invalid_producer(state_t const s) noexcept {return s + 1;}
    static constexpr state_t invalid_consumer(state_t const s) noexcept {return pushed(s);}
    static constexpr state_t closed(state_t const s) noexcept {
        return pushed(s) + (!single_consumer && !node_ref_ok);
    }

    template <typename U>
    static constexpr void pop_item(U&& v, T& item) noexcept {
        if constexpr(std::is_nothrow_assignable_v<U, T&&>) {
            std::forward<U>(v) = std::move(item);
        } else {
            std::forward<U>(v) = item;
        }
    }

    template <typename U>
    bool try_push_sp(U&& v) noexcept {
        auto curr_tail = tail_.load(mo::lax);
        TmpNodeRef<node_ref_ok> ref = {this, curr_tail.addr};

        for (;;) {
            auto& node = nodes_[curr_tail.addr];
            auto const slot_offset = curr_tail.addr * slots_per_node_;
            auto const enq_idx = node.enq_idx.load(mo::lax);
            if (enq_idx < slots_per_node_) {
                auto& slot = slots_[slot_offset + enq_idx];
                if constexpr (!single_consumer) {
                    // slot acquired, try pushing to slot
                    node.enq_idx.store(enq_idx + 1, mo::lax);
                    if (push_to_slot(v, slot, curr_tail, ref)) {
                        break;
                    }
                } else {
                    slot.item = std::forward<U>(v);
                    node.enq_idx.store(enq_idx + 1, mo::rls);
                    break;
                }
            } else if (!get_next_tail(curr_tail)) {
                return false; // full
            }

            if (node_ref_ok && ref.addr != curr_tail.addr) {
                ref.release();
                ref.addr = curr_tail.addr;
            }
        }

        return true;
    }

    template <typename Iter>
    size_t try_push_mp(Iter it, size_t const item_cnt) noexcept {
        size_t push_cnt = 0;
        auto curr_tail = tail_.load(mo::csm);
        TmpNodeRef<node_ref_ok> ref = {this, curr_tail.addr};

        for (;;) {
            auto& node = nodes_[curr_tail.addr];
            auto const slot_offset = curr_tail.addr * slots_per_node_;
            auto enq_idx = node.enq_idx.fetch_add(item_cnt, mo::acq);
            if (enq_idx < slots_per_node_) {
                auto slot_cnt = item_cnt;
                do {
                    // slot acquired, try pushing to slot
                    auto& slot = slots_[slot_offset + enq_idx];
                    auto const pushed = push_to_slot(*it, slot, curr_tail, ref);
                    it += pushed;
                    push_cnt += pushed;
                } while (--slot_cnt > 0 && ++enq_idx < slots_per_node_);

                if (push_cnt > 0) {
                    break;
                } else {
                    // unsuccessful pushes, possibly due to ABA, reload and try again
                    curr_tail = tail_.load(mo::csm);
                    continue;
                }
            }

            if (enq_idx >= slots_per_node_ && !get_next_tail(curr_tail)) {
                break; // full
            }

            if (node_ref_ok && ref.addr != curr_tail.addr) {
                ref.release();
                ref.addr = curr_tail.addr;
            }
        }

        return push_cnt;
    }

    template <typename U>
    size_t try_pop_sc(U&& v) noexcept {
        auto curr_head = head_.load(mo::lax);
        TmpNodeRef<node_ref_ok> ref = {this, curr_head.addr};

        for (;;) {
            auto& node = nodes_[curr_head.addr];
            auto const slot_offset = curr_head.addr * slots_per_node_;
            auto const deq_idx = node.deq_idx.load(mo::lax);
            auto const enq_idx = node.enq_idx.load(single_producer ? mo::acq : mo::lax);
            if (deq_idx < enq_idx && deq_idx < slots_per_node_) {
                node.deq_idx.store(deq_idx + 1, mo::lax);
                auto& slot = slots_[slot_offset + deq_idx];
                if constexpr (!single_producer) {
                    // slot acquired, try popping from slot
                    if (pop_from_slot(v, slot, curr_head, ref)) {
                        break;
                    }
                } else {
                    pop_item(v, slot.item);
                    break;
                }
            } else if (deq_idx < slots_per_node_ || !get_next_head(curr_head)) { 
                return false; // empty
            }
        }

        return true;
    }

    template <typename Iter, typename NodeRefType>
    bool try_pop_mc(Iter it, size_t const item_cnt, NodeRefType&& ref) noexcept {
        size_t pop_cnt = 0;
        auto curr_head = head_.load(mo::csm);

        for (;;) {
            begin_loop:
            if (node_ref_ok && ref.addr != curr_head.addr) {
                ref.release();
                ref.addr = curr_head.addr;
            }

            auto& node = nodes_[curr_head.addr];
            auto const slot_offset = curr_head.addr * slots_per_node_;
            size_t deq_idx, enq_idx;
            deq_idx = 0;
            enq_idx = node.enq_idx.load(mo::lax);
            while (enq_idx < slots_per_node_ && deq_idx < enq_idx) {
                auto const new_idx = deq_idx + std::min(item_cnt, enq_idx - deq_idx);
                if (node.deq_idx.compare_exchange_weak(deq_idx, new_idx, mo::acq, mo::lax)) {
                    do {
                        // slot acquired, try popping from slot
                        auto& slot = slots_[slot_offset + deq_idx];
                        auto const popped = pop_from_slot(*it, slot, curr_head, ref);
                        it += popped;
                        pop_cnt += popped;
                    } while (++deq_idx < new_idx);

                    if (pop_cnt > 0) {
                        goto exit;
                    } else {
                        // unsuccessful pops, possibly due to ABA, reload and try again
                        curr_head = head_.load(mo::csm);
                        goto begin_loop;
                    }
                }
                enq_idx = node.enq_idx.load(mo::lax);
            }

            if (deq_idx >= enq_idx) {
                deq_idx = node.deq_idx.load(mo::acq);
                if (deq_idx >= slots_per_node_) {
                    if (!get_next_head(curr_head)) {
                        break; // empty
                    }
                } else {
                    enq_idx = node.enq_idx.load(mo::acq);
                    auto const prev_head = curr_head;
                    curr_head = head_.load(mo::csm);
                    if (deq_idx >= enq_idx && prev_head == curr_head) {
                        break; // empty
                    }
                }
                continue;
            }

            // producers have filled the block, attempt dequeue optimistically
            deq_idx = node.deq_idx.fetch_add(item_cnt, mo::acq);
            if (deq_idx < slots_per_node_) {
                auto slot_cnt = item_cnt;
                do {
                    // slot acquired, try popping from slot
                    auto& slot = slots_[slot_offset + deq_idx];
                    auto const popped = pop_from_slot(*it, slot, curr_head, ref);
                    it += popped;
                    pop_cnt += popped;
                } while (--slot_cnt > 0 && ++deq_idx < slots_per_node_);

                if (pop_cnt > 0) {
                    break;
                } else {
                    // unsuccessful pops, possibly due to ABA, reload and try again
                    curr_head = head_.load(mo::csm);
                    continue;
                }
            }

            if (deq_idx >= slots_per_node_ && !get_next_head(curr_head)) {
                break; // empty
            }
        }

        exit:
        return pop_cnt;
    }

    template <typename U, bool sc = single_consumer, std::enable_if_t<sc, int> = 0>
    bool push_to_slot(U&& v, Slot& slot, TaggedPtr ptr, TmpNodeRef<node_ref_ok>&) noexcept {
        auto const s = nodes_[ptr.addr].state;
        if (s != ptr.state) {
            // producer ABA occurred, invalidate slot
            slot.state.store(invalid_producer(s), mo::lax);
            return false;
        }

        // everything looks good, push item
        slot.item = std::forward<U>(v);
        slot.state.store(pushed(s), mo::rls);
        return true;
    }

    template <typename U, bool sc = single_consumer, std::enable_if_t<!sc, int> = 0>
    bool push_to_slot(U&& v, Slot& slot, TaggedPtr ptr, TmpNodeRef<node_ref_ok>& ref) noexcept {
        auto const s = nodes_[ptr.addr].state;
        auto curr_state = open(s);
        if (!single_producer && s != ptr.state) {
            // producer ABA occurred, attempt to invalidate slot
            auto const new_state = invalid_producer(s);
            if (slot.state.compare_exchange_strong(curr_state, new_state, mo::lax, mo::lax)) {
                return false; // slot invalidated
            }
        } else {
            // everything looks good, attempt to push item
            slot.item = std::forward<U>(v);
            auto const new_state = pushed(s);
            if (slot.state.compare_exchange_strong(curr_state, new_state, mo::rls, mo::lax)) {
                return true; // success!
            }

            // consumer ABA occurred, recover item
            pop_item(v, slot.item);
        }

        // consumer ABA occurred, close slot if needed
        assert(curr_state == invalid_consumer(s));
        if (!node_ref_ok) {
            slot.state.store(closed(s), mo::rls);
        } else {
            assert(curr_state == closed(s));
            ++ref.cnt; // producer owns slot, increment reference count
        }
        return false;
    }

    template <typename U, typename NodeRefType>
    bool pop_from_slot(U&& v, Slot& slot, TaggedPtr ptr, NodeRefType&& ref) noexcept {
        auto const s = nodes_[ptr.addr].state;
        auto curr_state = open(s);
        if (!single_consumer && s != ptr.state) {
            // consumer ABA occurred, attempt to invalidate slot
            auto const new_state = invalid_consumer(s);
            if (slot.state.compare_exchange_strong(curr_state, new_state, mo::lax, mo::acq)) {
                return false; // slot invalidated
            }
        } else {
            // everything looks good, wait for producer
            while ((curr_state = slot.state.load(mo::acq)) == open(s))
                ;
        }

        if (node_ref_ok) {
            ++ref.cnt; // consumer owns slot, increment reference count
        }

        if (!single_producer && curr_state == invalid_producer(s)) {
            // producer ABA occurred, close slot
            slot.state.store(closed(s), mo::lax);
            return false;
        }

        // success! pop item and close slot if needed
        assert(curr_state == pushed(s));
        pop_item(v, slot.item);
        if (!single_consumer && !node_ref_ok) {
            slot.state.store(closed(s), mo::rls);
        } else {
            assert(curr_state == closed(s));
        }
        return true;
    }

    template <mo_t mem_ord = mo::lax>
    bool advance_ptr(std::atomic<TaggedPtr>& ptr, TaggedPtr& curr_val) noexcept {
        auto const new_val = nodes_[curr_val.addr].next.load(mo::csm);
        TaggedPtr const null_ptr = {null_addr, curr_val.state};
        if (new_val != null_ptr) {
            if (ptr.compare_exchange_weak(curr_val, new_val, mo::rls, mo::csm)) {
                remove_node_reference<mem_ord>(curr_val.addr);
                curr_val = new_val;
            }
            return true;
        }
        return false;
    }

    template <bool sp = single_producer, std::enable_if_t<sp, int> = 0>
    bool get_next_tail(TaggedPtr& curr_tail) noexcept {
        // attempt to allocate
        auto const alloc_addr = free_list_.try_alloc(nodes_);
        if (alloc_addr == null_addr) {
            return false; // free-list is empty
        }

        // allocation succeeded, prepare new node to be added
        auto& alloc_node = nodes_[alloc_addr];
        if (node_ref_ok) {
            alloc_node.reset(refs_per_node());
        } else {
            alloc_node.reset();
        }

        // append new node to end of the queue
        TaggedPtr alloc_ptr = {alloc_addr, alloc_node.state};
        nodes_[curr_tail.addr].next.store(alloc_ptr, mo::rls);

        // now that new node is added, advance the tail
        curr_tail = alloc_ptr;
        tail_.store(curr_tail, mo::lax);
        return true;
    }

    template <bool sp = single_producer, std::enable_if_t<!sp, int> = 0>
    bool get_next_tail(TaggedPtr& curr_tail) noexcept {
        // attempt to advance the tail first
        if (advance_ptr(tail_, curr_tail)) {
            return true;
        }

        // no more nodes in the queue, attempt to allocate
        auto const prev_tail = curr_tail;
        auto const alloc_addr = free_list_.try_alloc_or_advance_tail(nodes_, this, curr_tail);
        if (prev_tail != curr_tail) {
            return true; // tail was advanced already, no allocation needed
        } else if (alloc_addr == null_addr) {
            return false; // free-list is empty
        }

        // allocation succeeded, prepare new node to be added
        auto& alloc_node = nodes_[alloc_addr];
        alloc_node.reset(refs_per_node());

        // append new node to end of the queue 
        TaggedPtr alloc_ptr = {alloc_addr, alloc_node.state};
        auto curr_back = back_.load(mo::csm);
        auto done = false;
        do {
            auto& next_ptr = nodes_[curr_back.addr].next;
            auto curr_next = next_ptr.load(mo::csm);
            TaggedPtr const null_ptr = {null_addr, curr_back.state};
            if (curr_next == null_ptr) {
                if (next_ptr.compare_exchange_strong(curr_next, alloc_ptr, mo::rls, mo::csm)) {
                    curr_next = alloc_ptr;
                    done = true;
                }
            }
            if (back_.compare_exchange_weak(curr_back, curr_next, mo::rls, mo::csm)) {
                remove_node_reference(curr_back.addr);
            }
        } while(!done);

        // now that new node is added, advance the tail
        advance_ptr(tail_, curr_tail);
        return true;
    }

    template <bool sc = single_consumer, std::enable_if_t<sc, int> = 0>
    bool get_next_head(TaggedPtr& curr_head) noexcept {
        auto const curr_next = nodes_[curr_head.addr].next.load(mo::csm);
        TaggedPtr const null_ptr = {null_addr, curr_head.state};
        if (curr_next == null_ptr) {
            return false;
        }

        // next node is not null, advance the head, and remove the previous
        auto last_head = curr_head;
        curr_head = curr_next;
        head_.store(curr_head, mo::lax);
        remove_node_reference<mo::rls>(last_head.addr);
        return true;
    }

    template <bool sc = single_consumer, std::enable_if_t<!sc, int> = 0>
    bool get_next_head(TaggedPtr& curr_head) noexcept {
        return advance_ptr<mo::rls>(head_, curr_head);
    }

    template <mo_t mem_ord = mo::lax, bool sc = single_consumer, std::enable_if_t<sc, int> = 0>
    void remove_node_reference(address_t const addr, int const rmv_cnt = 1) noexcept {
        auto& node = nodes_[addr];
        if (single_producer || rmv_cnt == node.ref_cnt.fetch_sub(rmv_cnt, mem_ord)) {
            std::atomic_thread_fence(single_producer || mem_ord == mo::rls ? mo::lax : mo::acq);
            free_list_.dealloc(nodes_, addr);
        }
    }

    template <mo_t mem_ord = mo::lax, bool sc = single_consumer, std::enable_if_t<!sc, int> = 0>
    void remove_node_reference(address_t const addr, int const rmv_cnt = 1) noexcept {
        auto& node = nodes_[addr];
        auto const zero_ref_cnt = single_producer && !node_ref_ok;
        if (zero_ref_cnt || rmv_cnt == node.ref_cnt.fetch_sub(rmv_cnt, mem_ord)) {
            if (!node_ref_ok) {
                // wait for all slots to be closed
                auto const slot_offset = addr * slots_per_node_;
                size_t closed_cnt;
                do {
                    closed_cnt = 0;
                    for (size_t i = 0; i < slots_per_node_; ++i) {
                        auto const& slot = slots_[slot_offset + i];
                        closed_cnt += slot.state.load(mo::lax) == closed(node.state);
                    }
                } while (closed_cnt < slots_per_node_);
            }

            std::atomic_thread_fence(mo::acq); // synchronize with all releases
            free_list_.dealloc(nodes_, addr);
        }
    }

    constexpr auto refs_per_node() const noexcept {
        // head_, tail_, back_ = 3 pointers
        auto const num_ptrs = single_producer ? 1 : 3;
        return node_ref_ok ? slots_per_node_ + num_ptrs : num_ptrs;
    }

    struct alignas(uintptr_t) TaggedPtr {
        address_t addr;
        state_t state;

        bool operator==(TaggedPtr const& other) const noexcept {
            return addr == other.addr && state == other.state;
        }

        bool operator!=(TaggedPtr const& other) const noexcept {
            return addr != other.addr || state != other.state;
        }
    };
    static_assert(sizeof(TaggedPtr) == alignof(TaggedPtr));
    static_assert(sizeof(TaggedPtr) == sizeof(address_t) + sizeof(state_t));

    struct alignas(interference_size) StatelessSlot {
        T item;
    };

    struct alignas(interference_size) StatefulSlot {
        std::atomic<state_t> state = {0};
        T item;
    };

    struct Node {
        Node(size_t const init_ref_cnt) noexcept : ref_cnt{init_ref_cnt} {next = {null_addr, 0};}

        void reset(size_t const init_ref_cnt) noexcept {
            ref_cnt.store(init_ref_cnt, mo::lax);
            reset();
        }

        void reset() noexcept {
            std::atomic_thread_fence(single_producer && !single_consumer ? mo::rls : mo::lax);
            enq_idx.store(0, single_producer ? mo::lax : mo::rls);
            deq_idx.store(0, single_consumer ? mo::lax : mo::rls);
            next.store({null_addr, state}, mo::lax);
        }

        alignas(interference_size) std::atomic<TaggedPtr> next;
        alignas(interference_size) std::atomic<size_t> enq_idx = {0};
        alignas(interference_size) std::atomic<size_t> deq_idx = {0};
        alignas(interference_size) std::atomic<size_t> ref_cnt;
        alignas(interference_size) state_t state = 0;
    };

    struct FreeList {
        void dealloc(Node* nodes, address_t const addr) noexcept {
            // close state of node and make it new list head
            auto& node = nodes[addr];
            node.state = closed(node.state);
            TaggedPtr new_head = {addr, node.state};

            // append new head to list
            auto curr_head = head.load(mo::lax);
            do {
                node.next.store(curr_head, mo::lax);
            } while (!head.compare_exchange_weak(curr_head, new_head, mo::rls, mo::lax));
        }

        address_t try_alloc(Node* nodes) noexcept {
            TaggedPtr unused = {null_addr, 0};
            return try_alloc_or_advance_tail<false>(nodes, nullptr, unused);
        }

        template <bool advance_tail = true>
        address_t try_alloc_or_advance_tail(Node* nodes, Queue* q, TaggedPtr& curr_tail) noexcept {
            auto curr_head = head.load(mo::acq);

            // check if list is empty
            if (curr_head.addr == null_addr) {
                if (advance_tail) {
                    q->advance_ptr(q->tail_, curr_tail);
                }
                return null_addr;
            }

            // try to allocate a node or advance the tail
            do {
                TaggedPtr new_head = nodes[curr_head.addr].next.load(mo::lax);
                if (head.compare_exchange_weak(curr_head, new_head, mo::lax, mo::acq)) {
                    return curr_head.addr; // success!
                } else if (advance_tail && q->advance_ptr(q->tail_, curr_tail)) {
                    return null_addr; // pointer advanced, no need for allocation
                }
            } while (curr_head.addr != null_addr);

            return null_addr;
        }

        std::atomic<TaggedPtr> head;
    };

    NodeAlloc node_alloc_;
    SlotAlloc slot_alloc_;
    size_t num_nodes_; // constant after construction
    size_t slots_per_node_; // constant after construction
    Node* nodes_; // constant after construction
    Slot* slots_; // constant after construction
    alignas(interference_size) FreeList free_list_;
    alignas(interference_size) std::atomic<TaggedPtr> head_;
    alignas(interference_size) std::atomic<TaggedPtr> tail_;
    alignas(interference_size) std::atomic<TaggedPtr> back_;

    friend QueuePtr<T, Alloc, single_producer, single_consumer, node_ref_ok>
        ::QueuePtr(size_t, size_t, Alloc const&);
};

}
