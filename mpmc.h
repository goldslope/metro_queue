#pragma once

#include <atomic>

using std::size_t;

template <typename T,
          auto node_ref_usage=MetroQueue::NodeRef::disabled,
          auto progress_guarantee=MetroQueue::ProgressGuarantee::blocking>
class MetroQueuePtr {
    typedef MetroQueue<T, node_ref_enabled, progress_guarantee>> queue_t;

public:
    MetroQueuePtr(size_t queue_capacity, size_t node_capacity) :
        q_ptr(make_shared<queue_t>(MetroQueue::Key{0}, queue_capacity, node_capacity)) {}

    std::shared_ptr<queue_t>& operator->() const noexcept {return q_ptr;}

    void swap(MetroQueuePtr& other) {q_ptr.swap(other);}
    bool operator==(MetroQueuePtr& const other) const noexcept {return q_ptr == other.q_ptr;}
    bool operator!=(MetroQueuePtr& const other) const noexcept {return q_ptr != other.q_ptr;}

private:
    queue_t* const get() const noexcept {return q_ptr.get();}

    // non-movable
    MetroQueuePtr(MetroQueuePtr&&) = delete;
    MetroQueuePtr& operator=(MetroQueuePtr&&) = delete;

    // disable addressing and heap allocation
    MetroQueuePtr* operator&() = delete;
    void* operator new(size_t) = delete;
    void* operator new(size_t, std::align_val_t) = delete;
    void operator delete(void*) = delete;
    void operator delete(void*, std::align_val_t) = delete;

    std::shared_ptr<queue_t> const q_ptr;

    friend class MetroQueue;
};

template <typename T, bool node_ref_ok, bool obstruction_free>
class MetroQueue {
public:
    class NodeRef {
    public:
        enum : bool enabled_or_disabled {enabled = true, disabled = false};

        explicit NodeRef(MetroQueuePtr<T, true> q) noexcept : q_ptr(q), addr(null_addr), cnt(0) {}
        ~NodeRef() {release();}

        release() noexcept {
            if (cnt > 0 && addr != null_addr) {
                q_ptr->remove_node_reference(addr, cnt);
                cnt = 0;
            }
        }

        // non-copyable and non-movable
        NodeRef(NodeRef const&) = delete;
        NodeRef& operator=(NodeRef const&) = delete;

        // disable addressing and heap allocation
        NodeRef* operator&() = delete;
        void* operator new(size_t) = delete;
        void* operator new(size_t, std::align_val_t) = delete;
        void operator delete(void*) = delete;
        void operator delete(void*, std::align_val_t) = delete;

    private:
        MetroQueuePtr<T, true> const q_ptr;
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

public:
    MetroQueue(std::enable_if_t<is_nothrow_move_assignable_v<T>, Key>& const,
               size_t queue_capacity,
               size_t node_capacity) {

        node_capacity = std::max(node_capacity, 1);

        // determine number of nodes
        auto num_nodes = round_divide(queue_capacity, node_capacity)
        num_nodes = std::min(num_nodes, null_addr);
        num_nodes = std::max(num_nodes, 2);

        // determine slots per node
        slots_per_node_ = round_divide(queue_capacity, num_nodes);
        slots_per_node_ = std::max(slots_per_node_, node_capacity);

        // setup free-list
        nodes_ = new Node[num_nodes];
        nodes_[0].init();
        for (size_t i = 1; i < num_nodes; ++i) {
            nodes_[i].init();
            nodes_[i].free_next_ = i - 1;
        }
        free_list.head = {num_nodes - 1};

        // setup queue
        auto const init_addr = free_list_.try_pop(Node());
        head_ = {init_addr, 0};
        tail_ = {init_addr, 0};
        back_ = {init_addr, 0};
    }

    MetroQueue() = delete;
    ~MetroQueue() {delete[] nodes_;}

private:
    template <typename U>
    static constexpr auto is_valid_push_value =
        is_same_v<std::remove_reference_t<U>, T> &&
        is_nothrow_assignable_v<T, U>

    template <typename Iter>
    static constexpr auto is_valid_base_iterator =
        is_same_v<std::iterator_traits<Iter>::value_type, T> &&
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
        return try_pop(v, 1, TmpNodeRef<node_ref_ok>{this});
    }

    template <typename = std::enable_if_t<node_ref_ok>>
    bool try_pop(T& v, NodeRef& ref) noexcept {
        if (ref.q_ptr.get() != this) {
            return try_pop(v, 1, TmpNodeRef<node_ref_ok>{this});
        }

        return try_pop(v, 1, ref);
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

    static constexpr auto cache_line_size = 128;
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
            auto enq_idx = node.enq_idx.fetch_add(item_cnt, mo::acq);
            if (enq_idx > slots_per_node_) {
                auto slot_cnt = item_cnt;
                do {
                    // slot acquired, try pushing to slot
                    bool pushed;
                    auto const& slot = node.slots[enq_idx];
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
            size_t deq_idx, enq_idx;
            deq_idx = 0;
            enq_idx = node.enq_idx.load(mo::lax);
            while (enq_idx < slots_per_node_ && deq_idx < enq_idx) {
                auto const new_idx = deq_idx + std::min(item_cnt, enq_idx - deq_idx);
                if (node.deq_idx.compare_exchange_weak(deq_idx, new_idx, mo::acq, mo::lax)) {
                    do {
                        // slot acquired, try popping from slot
                        bool popped;
                        auto const& slot = node.slots[deq_idx];
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
                    auto const& slot = node.slots[deq_idx];
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
        auto const prev_cnt = node.ref_cnt.fetch_sub(rmv_cnt, node_ref_ok ? mo::rls : mo::lax);
        if (prev_cnt == rmv_cnt) {
            if (!node_ref_ok) {
                // wait for all slots to be closed
                for (size_t i = 0; i < slots_per_node_; ++i) {
                    while (node.slots[i].state != closed(node.state))
                        ;
                }
            }
            std::atomic_thread_fence(mo::acq); // synchronize with all releases
            free_list_.push(addr);
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
        auto const alloc_addr = free_list_.try_pop(curr_tail);
        if (curr_tail.addr == null_addr) {
            curr_tail = tail_.load(mo::csm);
            return true;
        } else if (alloc_addr == null_addr) {
            return false; // free-list is empty
        }

        // allocation succeeded, prepare new node to be added
        auto& alloc_node = nodes_[alloc_addr];
        alloc_node.reset();

        // append new node to end of the queue 
        TaggedPtr alloc_ptr = {alloc_addr, alloc_node.state};
        auto curr_back = back_.load(mo::csm);
        auto done = false;
        while (!done) {
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
        }

        // now that new node is added, advance the tail
        advance_ptr(tail_, curr_tail);
        return true;
    }

    struct alignas(cache_line_size) Slot {
        std::atomic<state_t> state = 0;
        T item;
    };

    struct TaggedPtr {
        address_t addr;
        state_t state;
    };

    struct Node {
        void init (size_t const capacity) {
            num_slots = capacity
            slots = new Slot[num_slots]
            ref_cnt = num_refs()
        }
        ~Node() {delete[] slots;}

        void reset () noexcept {
            state = closed(state);
            ref_cnt.store(num_refs(), mo::lax);
            enq_idx.store(0, mo::rls); // release to synchronize with producers
            deq_idx.store(0, mo::rls); // release to synchronize with consumers
            next.store({null_addr, state}, mo::lax); // not included in synchronization
        }

        // head_, tail_, back_ = 3 pointers = 3 references
        constexpr auto num_refs() const noexcept {return node_ref_ok ? num_slots + 3 : 3;}

        size_t num_slots; // constant after init()
        Slot* slots = nullptr; // constant after init()
        state = 0;

        // atomics
        alignas(cache_line_size) std::atomic<TaggedPtr> next = {null_addr, 0};
        alignas(cache_line_size) std::atomic<address_t> free_next = null_addr;
        std::atomic<size_t> ref_cnt; // same cacheline as free next
        alignas(cache_line_size) std::atomic<size_t> enq_idx = 0;
        alignas(cache_line_size) std::atomic<size_t> deq_idx = 0;
    };

    struct FreeList {
        void push(address_t const new_addr) noexcept {
            auto curr_head = head.load(mo::lax);
            TaggedPtr new_head = {new_addr};
            do {
                new_head.state = curr_head.state;
                nodes_[new_addr].free_next.store(curr_head.addr, mo::lax);
            } while (!head.compare_exchange_weak(curr_head, new_head, mo::rls, mo::lax));
        }

        address_t try_pop(TaggedPtr const& q_tail) noexcept {
            auto curr_head = head.load(mo::csm);
            auto const& q_tail_node = nodes_[q_tail.addr];
            TaggedPtr const null_ptr = {null_addr, q_tail.state};
            TaggedPtr new_head;
 
            // check for empty list on each loop iteration
            while (curr_head.addr != null_addr) {
                new_head.addr = nodes_[curr_head.addr].free_next.load(mo::lax);
                new_head.state = curr_head.state + 1;
                if (head.compare_exchange_weak(curr_head, new_head, mo::rls, mo::csm)) {
                    break; // success!
                } else if (q_tail_node.next.load(mo::lax) != null_ptr) {
                    q_tail.addr = null_addr; // indicate allocation is no longer needed
                    break;
                }
            }
            return curr_head.addr;
        }

        std::atomic<TaggedPtr> head;
    };

    size_t slots_per_node_; // constant after construction
    Node* nodes_; // constant after construction
    alignas(cache_line_size) free_list_;
    alignas(cache_line_size) std::atomic<TaggedPtr> head_;
    alignas(cache_line_size) std::atomic<TaggedPtr> tail_;
    alignas(cache_line_size) std::atomic<TaggedPtr> back_;

    friend MetroQueuePtr::MetroQueuePtr(size_t, size_t);
};
