#pragma once

#include <atomic>

using std::size_t;

template <typename T,
          MetroQueue::NodeRef::enabled_or_disabled node_ref_usage=MetroQueue::NodeRef::disabled,
          MetroQueue::ProgressGuarantee progress_guarantee=MetroQueue::ProgressGuarantee::blocking,
          typename Alloc>
class MetroQueuePtr {
    using QueueType = MetroQueue<T, node_ref_enabled, progress_guarantee>>;

public:
    MetroQueuePtr(size_t queue_capacity,
                  size_t node_capacity,
                  Alloc const& alloc=std::allocator<void>()) :
        q_ptr(allocate_shared<QueueType>(alloc,
                                       MetroQueue::Key{0},
                                       queue_capacity,
                                       node_capacity,
                                       alloc)) {}

    std::shared_ptr<QueueType>& operator->() const noexcept {return q_ptr;}

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

    friend class MetroQueue;
};

template <typename T, bool node_ref_ok, bool obstruction_free, typename Alloc>
class MetroQueue {
public:
    class NodeRef {
    public:
        enum : bool enabled_or_disabled {enabled = true, disabled = false};

        explicit NodeRef(MetroQueuePtr const q) noexcept :
            q_ptr(q), addr(null_addr), cnt(0) {}
        ~NodeRef() {release();}

        release() noexcept {
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
        MetroQueuePtr const q_ptr; // const member -> non-assignable
        address_t addr;
        size_t cnt;

        friend class MetroQueue;
    };

    enum class : bool ProgressGuarantee {obstruction_free = true, blocking = false}

private:
    template <bool enabled>
    struct TmpNodeRef {
        explicit TmpNodeRef(MetroQueue* q) noexcept : q_ptr(q), addr(null_addr), cnt(0) {}
        ~TmpNodeRef() {release();}

        release() noexcept {
            if (enabled && cnt > 0 && addr != null_addr) {
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
        TmpIterator(U&& v) noexcept : val(v) {}
        U&& operator*() const noexcept {return val;}
        TmpIterator& operator+=() const noexcept {}

        U&& val;
    };

private:
    struct Key {
        explicit Key(int) {}
    };

    using NodeAlloc = std::allocator_traits<Alloc>::rebind_alloc<Node>;
    using SlotAlloc = std::allocator_traits<Alloc>::rebind_alloc<Slot>;

public:
    MetroQueue(std::enable_if_t<is_nothrow_move_assignable_v<T>, Key> const&,
               size_t queue_capacity,
               size_t node_capacity,
               Alloc const& alloc) : alloc_(alloc) {

        node_capacity = std::max(node_capacity, 1);

        // determine number of nodes
        num_nodes_ = round_divide(queue_capacity, node_capacity)
        num_nodes_ = std::min(num_nodes_, null_addr);
        num_nodes_ = std::max(num_nodes_, 2);

        // determine slots per node
        slots_per_node_ = round_divide(queue_capacity, num_nodes_);
        slots_per_node_ = std::max(slots_per_node_, node_capacity);

        // setup allocators
        NodeAlloc node_alloc{alloc_};
        SlotAlloc slot_alloc{alloc_};

        // allocate and initialize nodes
        nodes_ = std::allocator_traits<NodeAlloc>.allocate(node_alloc, num_nodes_);
        for (size_t i = 0; i < num_nodes_; ++i) {
            std::allocator_traits<NodeAlloc>.construct(node_alloc, &nodes_[i], refs_per_node());
        }

        // allocate and initialize slots
        auto const num_slots = num_nodes_ * slots_per_node_;
        slots_ = std::allocator_traits<SlotAlloc>.allocate(slot_alloc, num_slots);
        for (size_t i = 0; i < num_slots; ++i) {
            std::allocator_traits<SlotAlloc>.construct(slot_alloc, &slots_[i]);
        }

        // setup free-list
        for (size_t i = 1; i < num_nodes_; ++i) {
            nodes_[i].free_next_ = i - 1;
        }
        free_list.head = {num_nodes_ - 1};

        // setup queue
        auto const init_addr = free_list_.try_pop();
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
            std::allocator_traits<NodeAlloc>.destroy(node_alloc, &nodes_[i]);
        }
        std::allocator_traits<NodeAlloc>.deallocate(node_alloc, nodes_, num_nodes_);

        // destroy and deallocate slots
        auto const num_slots = num_nodes_ * slots_per_node_;
        for (size_t i = 0; i < num_slots; ++i) {
            std::allocator_traits<SlotAlloc>.destroy(slot_alloc, &slots_[i]);
        }
        slots_ = std::allocator_traits<SlotAlloc>.deallocate(slot_alloc, slots_, num_slots);
    }

private:
    template <typename U>
    static constexpr auto is_valid_push_value =
        is_nothrow_assignable_v<T, U>

    template <typename Iter>
    static constexpr auto is_valid_base_iterator =
        is_same_v<std::iterator_traits<Iter>::iterator_category, std::random_access_iterator_tag>

    template <typename Iter>
    static constexpr auto is_valid_push_iterator =
        is_valid_base_iterator<Iter> &&
        is_nothrow_assignable_v<T, std::iterator_traits<Iter>::reference>

    template <typename Iter>
    static constexpr auto is_valid_pop_iterator =
        is_valid_base_iterator<Iter> &&
        !is_const<std::iterator_traits<Iter>::reference>

public:
    template <typename U, typename = std::enable_if_t<is_valid_push_value<U>>>
    bool try_push(U&& v) noexcept {
        return try_push(TmpIterator{std::forward<U>(v)}, 1);
    }

    bool try_pop(T& v) noexcept {
        return try_pop(TmpIterator{v}, 1, TmpNodeRef<node_ref_ok>{this});
    }

    template <typename = std::enable_if_t<node_ref_ok>>
    bool try_pop(T& v, NodeRef& ref) noexcept {
        if (ref.q_ptr.get() != this) {
            return try_pop(TmpIterator{v}, 1, TmpNodeRef<node_ref_ok>{this});
        }

        return try_pop(TmpIterator{v}, 1, ref);
    }

    template <typename Iter,
              typename = std::enable_if_t<is_valid_push_iterator<Iter>>>
    size_t try_push(Iter begin, Iter end) noexcept {
        auto const item_cnt = end - begin;
        if (item_cnt > 0) {
            return try_push(begin, item_cnt);
        }

        return 0;
    }

    template <typename Iter,
              typename = std::enable_if_t<is_valid_pop_iterator<Iter>>>
    size_t try_pop(Iter begin, Iter end) noexcept {
        auto const item_cnt = end - begin;
        if (item_cnt > 0) {
            return try_pop(begin, item_cnt, TmpNodeRef<node_ref_ok>{this});
        }

        return 0;
    }

    template <typename Iter,
              typename = std::enable_if_t<is_valid_pop_iterator<Iter> && node_ref_ok>>
    size_t try_pop(Iter begin, Iter end, NodeRef& ref) noexcept {
        auto const item_cnt = end - begin;
        if (item_cnt > 0) {
            if (ref.q_ptr.get() != this) {
                return try_pop(begin, item_cnt, TmpNodeRef<node_ref_ok>{this});
            }

            return try_pop(begin, item_cnt, ref);
        }

        return 0;
    }

private:
    enum class mo {
        lax = std::memory_order_relaxed,
        csm = std::memory_order_acquire, // consume = acquire, for now
        acq = std::memory_order_acquire,
        rls = std::memory_order_release,
    }

    static constexpr auto no_false_sharing_alignment = std::hardware_destructive_interference_size;
    static constexpr auto null_addr = std::numeric_limits<address_t>::max();
    static constexpr auto round_divide(size_t const a, size_t const b) noexcept {
        auto const max_a = std::numeric_limits<size_t>::max();
        auto const round_a = std::min(a, max_a - b + 1) + b - 1;
        return round_a / b;
    }
    static constexpr state_t open(state_t const s) noexcept {return s;}
    static constexpr state_t pushed(state_t const s) noexcept {return s + 2;}
    static constexpr state_t invalid_producer(state_t const s) noexcept {return s + 1;}
    static constexpr state_t invalid_consumer(state_t const s) noexcept {return s + 2;}
    static constexpr state_t closed(state_t const s) noexcept {return s + (node_ref_ok ? 2 : 3);}

    template <typename Iter>
    size_t try_push(Iter it, size_t const item_cnt) noexcept {
        size_t push_cnt = 0;
        auto curr_tail = tail_.load(mo::csm);
        auto& ref = TmpNodeRef<node_ref_ok>{this};

        for (;;) {
            auto& node = nodes_[curr_tail.addr];
            auto const slot_offset = curr_tail.addr * slots_per_node_;
            auto enq_idx = node.enq_idx.fetch_add(item_cnt, mo::acq);
            if (enq_idx > slots_per_node_) {
                auto slot_cnt = item_cnt;
                do {
                    // slot acquired, try pushing to slot
                    bool pushed;
                    auto const& slot = slots_[slot_offset + enq_idx];
                    if (node_ref_ok) {
                        pushed = push_to_slot(*it, slot, curr_tail, ref);
                    } else {
                        pushed = push_to_slot(*it, slot, curr_tail);
                    }
                    it += pushed;
                    push_cnt += pushed;
                } while (--slot_cnt > 0 && ++enq_idx < slots_per_node_);

                if (push_cnt > 0) {
                    break;
                }
            }

            if (enq_idx >= slots_per_node_ && !get_next_tail(curr_tail)) {
                break; // full
            }
        }

        return push_cnt;
    }

    template <Iter it, typename NodeRefType>
    bool try_pop(Iter it, size_t const item_cnt, NodeRefType& ref) noexcept {
        size_t pop_cnt = 0;
        auto curr_head = head_.load(mo::csm);

        for (;;) {
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
                        bool popped;
                        auto const& slot = slots_[slot_offset + deq_idx];
                        if (node_ref_ok) {
                            popped = pop_from_slot(*it, slot, curr_head, ref);
                        } else {
                            popped = pop_from_slot(*it, slot, curr_head);
                        }
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
                    bool popped;
                    auto const& slot = slots_[slot_offset + deq_idx];
                    if (node_ref_ok) {
                        popped = pop_from_slot(*it, slot, curr_head, ref);
                    } else {
                        popped = pop_from_slot(*it, slot, curr_head);
                    }
                    it += popped;
                    pop_cnt += popped;
                } while (--slot_cnt > 0 && ++deq_idx < slots_per_node_);

                if (pop_cnt > 0) {
                    break;
                }
            }

            if (deq_idx >= slots_per_node_ && !advance_ptr(head_, curr_head)) {
                break; // empty
            }
        }

        return pop_cnt;
    }

    template <typename U>
    bool push_to_slot(U&& v, Slot& slot, TaggedPtr ptr, TmpNodeRef& ref) noexcept {
        auto const s = nodes_[ptr.addr].state;
        auto curr_state = open(s);
        if (s != ptr.state) {
            // producer ABA occurred, attempt to invalidate slot
            auto const new_state = invalid_producer(s);
            if (slot.state.compare_and_exchange_strong(curr_state, new_state, mo::rls, mo::acq)) {
                return false; // slot invalidated
            }
        } else {
            // everything looks good, attempt to write
            slot.item = std::forward<U>(v);
            auto const new_state = pushed(s);
            if (slot.state.compare_and_exchange_strong(curr_state, new_state, mo::rls, mo::acq)) {
                return true; // success!
            }

            // consumer ABA occurred, recover item
            v = std::move(slot.item);
        }

        // consumer ABA occurred, close slot if needed
        assert(curr_state == invalid_consumer(s));
        if (!node_ref_ok) {
            slot.state.store(closed(s), mo::rls);
        } else {
            assert(curr_state == closed(s))
            ++ref.cnt; // producer owns slot, increment reference count
        }
        return false;
    }

    template <typename NodeRefType>
    bool pop_from_slot(T& item, Slot& slot, TaggedPtr ptr, NodeRefType& ref) noexcept {
        auto const s = nodes_[ptr.addr].state;
        auto curr_state = open(s);
        if (obstruction_free || s != ptr.state) {
            // consumer ABA occurred, attempt to invalidate slot
            auto const new_state = invalid_consumer(s);
            if (slot.state.compare_and_exchange_strong(curr_state, new_state, mo::rls, mo::acq)) {
                return false; // slot invalidated
            }
        } else {
            // everything looks good, attempt to read
            while ((curr_state = slot.state.load(mo::acq)) == open(s))
                ;
        }

        if (node_ref_ok) {
            ++ref.cnt; // consumer owns slot, increment reference count
        }

        if (curr_state == invalid_producer(s)) {
            // producer ABA occurred, close slot
            slot.state.store(closed(s), node_ref_ok ? mo::lax : mo::rls);
            return false;
        }

        // success! grab item and close slot if needed
        assert(curr_state == pushed(s));
        v = std::move(slot.item);
        if (!node_ref_ok) {
            slot.state.store(closed(s), mo::rls);
        } else {
            assert(curr_state == closed(s))
        }
        return true;
    }

    void remove_node_reference(address_t const addr, int const rmv_cnt=1) noexcept {
        auto& node = nodes_[addr];
        auto const slot_offset = addr * slots_per_node_;
        auto const prev_cnt = node.ref_cnt.fetch_sub(rmv_cnt, node_ref_ok ? mo::rls : mo::lax);
        if (prev_cnt == rmv_cnt) {
            if (!node_ref_ok) {
                // wait for all slots to be closed
                size_t closed_cnt;
                do {
                    closed_cnt = 0
                    for (size_t i = 0; i < slots_per_node_; ++i) {
                        auto const& slot = slots_[slot_offset + i];
                        closed_cnt += slot.state.load(mo::lax) == closed(node.state)
                    }
                } while (closed_cnt < slots_per_node_);
            }
            std::atomic_thread_fence(mo::acq); // synchronize with all releases
            free_list_.dealloc(addr);
        }
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

    bool get_next_tail(TaggedPtr& curr_tail) noexcept {
        // attempt to advance the tail first
        if (advance_ptr(tail_, curr_tail)) {
            return true;
        }

        // no more nodes in the queue, attempt to allocate
        auto const alloc_addr = free_list_.try_alloc(curr_tail);
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

    constexpr auto refs_per_node() const noexcept {
        // head_, tail_, back_ = 3 pointers = 3 references
        return node_ref_ok ? slots_per_node_ + 3 : 3;
    }

    struct alignas(no_false_sharing_alignment) Slot {
        T item; // TODO: used aligned storage instead + constructor / destructor
        std::atomic<state_t> state = 0;
    };

    struct TaggedPtr {
        address_t addr;
        state_t state;
    };

    struct Node {
        Node (size_t const init_ref_cnt) : ref_cnt(init_ref_cnt) {}

        void reset (size_t const init_ref_cnt) noexcept {
            state = closed(state);
            ref_cnt.store(init_ref_cnt, mo::lax);
            enq_idx.store(0, mo::rls); // release to synchronize with producers
            deq_idx.store(0, mo::rls); // release to synchronize with consumers
            next.store({null_addr, state}, mo::lax); // not included in synchronization
        }

        alignas(no_false_sharing_alignment) std::atomic<TaggedPtr> next = {null_addr, 0};
        alignas(no_false_sharing_alignment) std::atomic<address_t> free_next = null_addr;
        state_t state = 0;
        alignas(no_false_sharing_alignment) std::atomic<size_t> enq_idx = 0;
        alignas(no_false_sharing_alignment) std::atomic<size_t> deq_idx = 0;
        alignas(no_false_sharing_alignment) std::atomic<size_t> ref_cnt;
    };

    struct FreeList {
        void dealloc(address_t const new_addr) noexcept {
            auto curr_head = head.load(mo::lax);
            TaggedPtr new_head = {new_addr};
            do {
                new_head.state = curr_head.state;
                nodes_[new_addr].free_next.store(curr_head.addr, mo::lax);
            } while (!head.compare_exchange_weak(curr_head, new_head, mo::rls, mo::lax));
        }

        address_t try_alloc() noexcept {
            try_alloc<false>({null_addr, 0});
        }

        template <bool q_tail_check=true>
        address_t try_alloc(TaggedPtr const& q_tail) noexcept {
            auto const& q_tail_node = nodes_[q_tail.addr];
            auto curr_head = head.load(mo::csm);
            TaggedPtr const null_ptr = {null_addr, q_tail.state};
            TaggedPtr new_head;
 
            // check for empty list on each loop iteration
            while (curr_head.addr != null_addr) {
                new_head.addr = nodes_[curr_head.addr].free_next.load(mo::lax);
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
    alignas(no_false_sharing_alignment) free_list_;
    alignas(no_false_sharing_alignment) std::atomic<TaggedPtr> head_;
    alignas(no_false_sharing_alignment) std::atomic<TaggedPtr> tail_;
    alignas(no_false_sharing_alignment) std::atomic<TaggedPtr> back_;

    friend MetroQueuePtr::MetroQueuePtr(size_t, size_t);
};
