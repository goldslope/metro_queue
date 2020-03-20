#pragma once

#include <algorithm>
#include <atomic>
#include <memory>
#include <type_traits>

using std::size_t;

template <typename,
          bool single_producer,
          bool single_consumer,
          bool node_ref_ok,
          bool relaxed_node_load,
          typename,
          typename = std::enable_if_t<!single_consumer || !node_ref_ok>,
          typename = std::enable_if_t<!relaxed_node_load ||
                                      !single_producer ||
                                      node_ref_ok>>
class MetroQueue;

template <typename T,
          bool single_producer = false,
          bool single_consumer = false,
          bool node_ref_ok = false,
          bool relaxed_node_load = false,
          typename Alloc = std::allocator<void>>
class MetroQueuePtr {
    
    using QueueType = MetroQueue<T,
                                 single_producer,
                                 single_consumer,
                                 node_ref_ok,
                                 relaxed_node_load,
                                 Alloc>;

private:
    struct Key {
        explicit Key(int) {}
    };

public:
    MetroQueuePtr(size_t queue_capacity,
                  size_t node_capacity,
                  Alloc const& alloc = std::allocator<void>{}) :
        q_ptr{std::allocate_shared<QueueType>(alloc,
                                              Key{0},
                                              queue_capacity,
                                              node_capacity,
                                              alloc)} {}

    std::shared_ptr<QueueType> const& operator->() const noexcept {return q_ptr;}

    bool operator==(MetroQueuePtr const& other) const noexcept {return q_ptr == other.q_ptr;}
    bool operator!=(MetroQueuePtr const& other) const noexcept {return q_ptr != other.q_ptr;}

private:
    QueueType* const get() const noexcept {return q_ptr.get();}

    // disable heap allocation
    void* operator new(size_t) = delete;
    void* operator new(size_t, std::align_val_t) = delete;
    void operator delete(void*) = delete;
    void operator delete(void*, std::align_val_t) = delete;

    std::shared_ptr<QueueType> const q_ptr; // single const member -> immutable

    friend class MetroQueue<T, single_producer, single_consumer, node_ref_ok, relaxed_node_load, Alloc>;
};

template <typename T,
          bool single_producer,
          bool single_consumer,
          bool node_ref_ok,
          bool relaxed_node_load,
          typename Alloc,
          typename,
          typename>
class MetroQueue {
    
    typedef uint32_t address_t;
    typedef uint32_t state_t;

    using QueuePtrType = MetroQueuePtr<T,
                                       single_producer,
                                       single_consumer,
                                       node_ref_ok,
                                       relaxed_node_load,
                                       Alloc>;

public:
    class NodeRef {
    public:
        //TODO: put the following in the right areas
        //enum : bool enabled_or_disabled {enabled = true, disabled = false};
        //MetroQueue::NodeRef::enabled_or_disabled node_ref_usage = MetroQueue::NodeRef::disabled,

        explicit NodeRef(QueuePtrType const& q) noexcept : q_ptr{q}, addr{null_addr}, cnt{0} {}
        ~NodeRef() {release();}

        void release() noexcept {
            if (cnt > 0 && addr != null_addr) {
                q_ptr->remove_node_reference(addr, cnt);
                cnt = 0;
            }
        }

        // disable copy and move constructor
        NodeRef(NodeRef const&) = delete;

        // disable heap allocation
        void* operator new(size_t) = delete;
        void* operator new(size_t, std::align_val_t) = delete;
        void operator delete(void*) = delete;
        void operator delete(void*, std::align_val_t) = delete;

    private:
        QueuePtrType const q_ptr; // const member -> non-assignable
        address_t addr;
        size_t cnt;

        friend class MetroQueue;
    };

private:
    template <bool enabled>
    struct TmpNodeRef {
        TmpNodeRef(MetroQueue* const q) noexcept : q_ptr{q}, addr{null_addr}, cnt{0} {}
        TmpNodeRef(MetroQueue* const q, address_t a) noexcept : q_ptr{q}, addr{a}, cnt{0} {}
        ~TmpNodeRef() {release();}

        template <bool fake_ref = !enabled, typename = std::enable_if_t<fake_ref>>
        constexpr void release() const noexcept {}

        template <bool real_ref = enabled, typename = std::enable_if_t<real_ref>>
        void release() noexcept {
            if (cnt > 0 && addr != null_addr) {
                q_ptr->remove_node_reference(addr, cnt);
                cnt = 0;
            }
        }

        MetroQueue* const q_ptr;
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
    struct Slot;
    struct Node;

    using NodeAlloc = typename std::allocator_traits<Alloc>::template rebind_alloc<Node>;
    using SlotAlloc = typename std::allocator_traits<Alloc>::template rebind_alloc<Slot>;

public:
    MetroQueue(typename QueuePtrType::Key const&,
               size_t queue_capacity,
               size_t node_capacity,
               Alloc const& alloc) : alloc_{alloc} {

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

        // setup allocators
        NodeAlloc node_alloc{alloc_};
        SlotAlloc slot_alloc{alloc_};

        // allocate and initialize nodes
        nodes_ = std::allocator_traits<NodeAlloc>::allocate(node_alloc, num_nodes_);
        for (size_t i = 0; i < num_nodes_; ++i) {
            std::allocator_traits<NodeAlloc>::construct(node_alloc, &nodes_[i], refs_per_node());
        }

        // allocate and initialize slots
        auto const num_slots = num_nodes_ * slots_per_node_;
        slots_ = std::allocator_traits<SlotAlloc>::allocate(slot_alloc, num_slots);
        for (size_t i = 0; i < num_slots; ++i) {
            std::allocator_traits<SlotAlloc>::construct(slot_alloc, &slots_[i]);
        }

        // setup free-list
        for (size_t i = 1; i < num_nodes_; ++i) {
            nodes_[i].free_next = i - 1;
        }
        free_list_.head = {static_cast<address_t>(num_nodes_ - 1), 0};

        // setup queue
        auto const init_addr = free_list_.try_alloc(nodes_);
        head_ = {init_addr, 0};
        tail_ = {init_addr, 0};
        back_ = {init_addr, 0};
    }

    ~MetroQueue() {
        // setup allocators
        NodeAlloc node_alloc{alloc_};
        SlotAlloc slot_alloc{alloc_};

        // destroy and deallocate nodes
        for (size_t i = 0; i < num_nodes_; ++i) {
            std::allocator_traits<NodeAlloc>::destroy(node_alloc, &nodes_[i]);
        }
        std::allocator_traits<NodeAlloc>::deallocate(node_alloc, nodes_, num_nodes_);

        // destroy and deallocate slots
        auto const num_slots = num_nodes_ * slots_per_node_;
        for (size_t i = 0; i < num_slots; ++i) {
            std::allocator_traits<SlotAlloc>::destroy(slot_alloc, &slots_[i]);
        }
        std::allocator_traits<SlotAlloc>::deallocate(slot_alloc, slots_, num_slots);
    }

private:
    template <typename U>
    static constexpr auto is_valid_pop_value =
        std::is_nothrow_assignable<U, T&>::value || std::is_nothrow_assignable<U, T&&>::value;

    template <typename U>
    static constexpr auto is_valid_push_value =
        std::is_nothrow_assignable<T&, U>::value && (single_consumer || is_valid_pop_value<U>);

    template <typename Iter>
    static constexpr auto is_valid_base_iterator =
        std::is_same<typename std::iterator_traits<Iter>::iterator_category,
                     std::random_access_iterator_tag>::value;

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
        if (single_producer) {
            return try_push_sp(v);
        }

        return try_push_mp(TmpIterator<U>{v}, 1);
    }

    template <typename U, typename = std::enable_if_t<is_valid_pop_value<U>>>
    bool try_pop(U&& v) noexcept {
        if (single_consumer) {
            return try_pop_sc(v);
        }

        return try_pop_mc(TmpIterator<U>{v}, 1, TmpNodeRef<node_ref_ok>{this});
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
    struct mo {
        static constexpr auto lax = std::memory_order_relaxed;
        static constexpr auto csm = std::memory_order_acquire; // consume = acquire, for now
        static constexpr auto acq = std::memory_order_acquire;
        static constexpr auto rls = std::memory_order_release;
        static constexpr auto acq_rls = std::memory_order_acq_rel;
    };

#ifdef __cpp_lib_hardware_interference_size
    static constexpr auto no_false_sharing_alignment = std::hardware_destructive_interference_size;
#else
    static constexpr auto no_false_sharing_alignment = 128;
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
        std::forward<U>(v) = std::is_nothrow_assignable<U, T&&>::value ? std::move(item) : item;
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
                // slot acquired, try pushing to slot
                auto& slot = slots_[slot_offset + enq_idx];
                auto const pushed = push_to_slot(v, slot, curr_tail, ref);
                node.enq_idx.store(enq_idx + 1, single_consumer ? mo::rls : mo::lax);

                if (pushed) {
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
        auto curr_tail = tail_.load(relaxed_node_load ? mo::lax : mo::csm);
        TmpNodeRef<node_ref_ok> ref = {this, curr_tail.addr};
        bool retry = true; // only for relaxed_node_load

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
                }
            }

            if (relaxed_node_load && retry) {
                retry = false;
                curr_tail = tail_.load(mo::csm);
                continue;
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
            if (deq_idx < enq_idx) {
                // slot acquired, try pushing to slot
                auto& slot = slots_[slot_offset + deq_idx];
                auto const popped = pop_from_slot(v, slot, curr_head, ref);
                node.deq_idx.store(deq_idx + 1, mo::lax);

                if (popped) {
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
        auto curr_head = head_.load(relaxed_node_load && node_ref_ok ? mo::lax : mo::csm);

        for (;;) {
            if (node_ref_ok) {
                if (relaxed_node_load && (ref.cnt == 0 || ref.addr != curr_head.addr)) {
                    curr_head = head_.load(mo::csm); // reload unrelaxed here
                }
                if (ref.addr != curr_head.addr) {
                    ref.release();
                    ref.addr = curr_head.addr;
                }
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
                        return pop_cnt;
                    }
                }
                enq_idx = node.enq_idx.load(mo::lax);
            }

            if (deq_idx >= enq_idx) {
                auto const prev_head = curr_head;
                curr_head = head_.load(mo::csm);
                if (prev_head == curr_head) {
                    break; // empty
                } else {
                    continue;
                }
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
                }
            }

            if (deq_idx >= slots_per_node_ && !get_next_head(curr_head)) {
                break; // empty
            }
        }

        return pop_cnt;
    }

    template <typename U, bool sc = single_consumer, std::enable_if_t<sc, int> = 0>
    bool push_to_slot(U&& v, Slot& slot, TaggedPtr ptr, TmpNodeRef<node_ref_ok>& ref) noexcept {
        auto const s = nodes_[ptr.addr].state;
        if (!single_producer && s != ptr.state) {
            // producer ABA occurred, invalidate slot
            slot.state.store(invalid_producer(s), mo::rls);
            return false;
        }

        // everything looks good, push item
        slot.item = std::forward<U>(v);
        if (!single_producer) {
            slot.state.store(pushed(s), mo::rls);
        }
        return true;
    }

    template <typename U, bool sc = single_consumer, std::enable_if_t<!sc, int> = 0>
    bool push_to_slot(U&& v, Slot& slot, TaggedPtr ptr, TmpNodeRef<node_ref_ok>& ref) noexcept {
        auto const s = nodes_[ptr.addr].state;
        auto curr_state = open(s);
        if (!single_producer && s != ptr.state) {
            // producer ABA occurred, attempt to invalidate slot
            auto const new_state = invalid_producer(s);
            if (slot.state.compare_exchange_strong(curr_state, new_state, mo::rls, mo::acq)) {
                return false; // slot invalidated
            }
        } else {
            // everything looks good, attempt to push item
            slot.item = std::forward<U>(v);
            auto const new_state = pushed(s);
            if (slot.state.compare_exchange_strong(curr_state, new_state, mo::rls, mo::acq)) {
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
            if (slot.state.compare_exchange_strong(curr_state, new_state, mo::rls, mo::acq)) {
                return false; // slot invalidated
            }
        } else if (!single_producer || !single_consumer) {
            // everything looks good, wait for producer
            while ((curr_state = slot.state.load(mo::acq)) == open(s))
                ;
        }

        if (node_ref_ok) {
            ++ref.cnt; // consumer owns slot, increment reference count
        }

        if (!single_producer && curr_state == invalid_producer(s)) {
            // producer ABA occurred, close slot
            slot.state.store(closed(s), single_consumer || node_ref_ok ? mo::lax : mo::rls);
            return false;
        }

        // success! pop item and close slot if needed
        assert((single_producer && single_consumer) || curr_state == pushed(s));
        pop_item(v, slot.item);
        if (!single_consumer && !node_ref_ok) {
            slot.state.store(closed(s), mo::rls);
        } else {
            assert((single_producer && single_consumer) || curr_state == closed(s));
        }
        return true;
    }

    bool advance_ptr(std::atomic<TaggedPtr>& ptr, TaggedPtr& curr_val) noexcept {
        auto const new_val = nodes_[curr_val.addr].next.load(mo::csm);
        TaggedPtr const null_ptr = {null_addr, curr_val.state};
        if (new_val != null_ptr) {
            if (ptr.compare_exchange_strong(curr_val, new_val, mo::rls, mo::csm)) {
                remove_node_reference(curr_val.addr);
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
        alloc_node.reset();

        // append new node to end of the queue
        TaggedPtr alloc_ptr = {alloc_addr, alloc_node.state};
        auto& next_ptr = nodes_[curr_tail.addr].next;
        next_ptr.store(alloc_ptr, mo::rls);

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
        auto const alloc_addr = free_list_.try_alloc(nodes_, curr_tail);
        if (curr_tail.addr == null_addr) {
            curr_tail = tail_.load(mo::csm);
            return true;
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
            if (back_.compare_exchange_strong(curr_back, curr_next, mo::rls, mo::csm)) {
                remove_node_reference(curr_back.addr);
            }
        } while(!done);

        // now that new node is added, advance the tail
        advance_ptr(tail_, curr_tail);
        return true;
    }

    template <bool sc = single_consumer, std::enable_if_t<sc, int> = 0>
    bool get_next_head(TaggedPtr& curr_head) noexcept {
        auto const& next_ptr = nodes_[curr_head.addr].next;
        auto const curr_next = next_ptr.load(mo::csm);
        TaggedPtr const null_ptr = {null_addr, curr_head.state};
        if (curr_next == null_ptr) {
            return false;
        }

        // next node is not null, advance the head, and remove the previous
        auto last_head = curr_head;
        curr_head = curr_next;
        head_.store(curr_head, mo::lax);
        remove_node_reference(last_head.addr);
        return true;
    }

    template <bool sc = single_consumer, std::enable_if_t<!sc, int> = 0>
    bool get_next_head(TaggedPtr& curr_head) noexcept {
        return advance_ptr(head_, curr_head);
    }

    template <bool sc = single_consumer, std::enable_if_t<sc, int> = 0>
    void remove_node_reference(address_t const addr, int const rmv_cnt = 1) noexcept {
        auto& node = nodes_[addr];
        if (single_producer || rmv_cnt == node.ref_cnt.fetch_sub(rmv_cnt,  mo::acq_rls)) {
            free_list_.dealloc(nodes_, addr);
        }
    }

    template <bool sc = single_consumer, std::enable_if_t<!sc, int> = 0>
    void remove_node_reference(address_t const addr, int const rmv_cnt = 1) noexcept {
        auto& node = nodes_[addr];
        auto const sub_memory_order = node_ref_ok ? mo::acq_rls : mo::lax;
        if (single_producer || rmv_cnt == node.ref_cnt.fetch_sub(rmv_cnt, sub_memory_order)) {
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
                std::atomic_thread_fence(mo::acq); // synchronize with all releases
            }

            free_list_.dealloc(nodes_, addr);
        }
    }

    constexpr auto refs_per_node() const noexcept {
        // head_, tail_, back_ = 3 pointers
        auto const num_ptrs = single_producer ? 0 : 3;
        return node_ref_ok ? slots_per_node_ + num_ptrs : num_ptrs;
    }

    struct TaggedPtr {
        address_t addr;
        state_t state;
    
        bool operator==(TaggedPtr const& other) const noexcept {return addr == other.addr && state == other.state;}
        bool operator!=(TaggedPtr const& other) const noexcept {return addr != other.addr || state != other.state;}
    };

    struct alignas(no_false_sharing_alignment) Slot {
        T item;
        std::atomic<state_t> state = {0};
    };

    struct Node {
        Node(size_t const init_ref_cnt) : ref_cnt{init_ref_cnt} {next = {null_addr, 0};}

        void reset(size_t const init_ref_cnt) noexcept {
            ref_cnt.store(init_ref_cnt, mo::lax);
            reset();
        }

        void reset() noexcept {
            enq_idx.store(0, mo::rls); // release to synchronize with producers
            deq_idx.store(0, mo::rls); // release to synchronize with consumers
            next.store({null_addr, state}, mo::lax); // not included in synchronization
        }

        alignas(no_false_sharing_alignment) std::atomic<TaggedPtr> next;
        alignas(no_false_sharing_alignment) std::atomic<address_t> free_next = {null_addr};
        state_t state = 0;
        alignas(no_false_sharing_alignment) std::atomic<size_t> enq_idx = {0};
        alignas(no_false_sharing_alignment) std::atomic<size_t> deq_idx = {0};
        alignas(no_false_sharing_alignment) std::atomic<size_t> ref_cnt;
    };

    struct FreeList {
        void dealloc(Node* nodes, address_t const addr) noexcept {
            auto curr_head = head.load(mo::lax);
            TaggedPtr new_head = {addr};
            do {
                new_head.state = curr_head.state;
                nodes[addr].state = closed(nodes[addr].state);
                nodes[addr].free_next.store(curr_head.addr, mo::lax);
            } while (!head.compare_exchange_weak(curr_head, new_head, mo::rls, mo::lax));
        }

        address_t try_alloc(Node* nodes) noexcept {
            TaggedPtr null_ptr = {null_addr, 0};
            return try_alloc<false>(nodes, null_ptr);
        }

        template <bool q_tail_check=true>
        address_t try_alloc(Node* nodes, TaggedPtr& q_tail) noexcept {
            auto const& q_tail_node = nodes[q_tail.addr];
            auto curr_head = head.load(mo::csm);
            TaggedPtr const null_ptr = {null_addr, q_tail.state};
            TaggedPtr new_head;
 
            // check for empty list on each loop iteration
            while (curr_head.addr != null_addr) {
                new_head.addr = nodes[curr_head.addr].free_next.load(mo::lax);
                new_head.state = curr_head.state + 1;
                if (head.compare_exchange_weak(curr_head, new_head, mo::rls, mo::csm)) {
                    break; // success!
                } else if (q_tail_check && q_tail_node.next.load(mo::lax) != null_ptr) {
                    q_tail.addr = null_addr; // indicate allocation is no longer needed
                    break;
                }
            }
            return curr_head.addr;
        }

        std::atomic<TaggedPtr> head;
    };

    Alloc const alloc_;
    size_t num_nodes_; // constant after construction
    size_t slots_per_node_; // constant after construction
    Node* nodes_; // constant after construction
    Slot* slots_; // constant after construction
    alignas(no_false_sharing_alignment) FreeList free_list_;
    alignas(no_false_sharing_alignment) std::atomic<TaggedPtr> head_;
    alignas(no_false_sharing_alignment) std::atomic<TaggedPtr> tail_;
    alignas(no_false_sharing_alignment) std::atomic<TaggedPtr> back_;

    friend MetroQueuePtr<T,
                         single_producer,
                         single_consumer,
                         node_ref_ok,
                         relaxed_node_load,
                         Alloc>::MetroQueuePtr(size_t, size_t, Alloc const&);
};
