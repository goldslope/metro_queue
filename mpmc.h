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

    template <typename... Args> void emplace(Args &&... args) noexcept {
    }

    template <typename... Args> bool try_emplace(Args &&... args) noexcept {
    }

    void push(const T &v) noexcept {
        emplace(v);
    }

    template <typename P,
            typename = typename std::enable_if<
                std::is_nothrow_constructible<T, P &&>::value>::type>
    void push(P &&v) noexcept {
        emplace(std::forward<P>(v));
    }

    bool try_push(const T &v) noexcept {
        return try_emplace(v);
    }

    template <typename P,
            typename = typename std::enable_if<
                std::is_nothrow_constructible<T, P &&>::value>::type>
    bool try_push(P &&v) noexcept {
        return try_emplace(std::forward<P>(v));
    }

    void pop(T &v) noexcept {
    }

    bool try_pop(T &v) noexcept {
    }

private:
    static constexpr size_t kCacheLineSize = 128;
    static constexpr size_t kNull = 0xFFFFFFFF;

    struct Slot {
        ~Slot() noexcept {
            if (turn & 1) {
                destroy();
            }
        } 

        template <typename... Args> void construct(Args &&... args) noexcept {
            new (&storage) T(std::forward<Args>(args)...);
        }

        void destroy() noexcept {
            reinterpret_cast<T *>(&storage)->~T();
        }

        T &&move() noexcept { return reinterpret_cast<T &&>(storage); }

        alignas(kCacheLineSize) std::atomic<size_t> turn = {0};
        typename std::aligned_storage<sizeof(T), alignof(T)>::type storage;
    };

    struct TaggedPtr {
        uint32_t idx;
        uint32_t tag;
    };

    struct Node {
        explicit Node() {
            size_t space = blocksize_ * sizeof(Slot) + kCacheLineSize - 1;
            buffer = malloc(space);
            if (buffer == kNull) {
                throw std::bad_alloc();
            }

            slots = reinterpret_cast<Slot *>(
                    std::align(
                        kCacheLineSize,
                        blocksize_ * sizeof(Slot),
                        buffer,
                        space));
            if (slots == kNull) {
                free(buffer);
                throw std::bad_alloc();
            }

            for (size_t i = 0; i < blocksize_; ++i) {
                new (&slots[i]) Slot();
            } 
        }

        ~Node() {
            for (size_t i = 0; i < blocksize_; ++i) {
                reinterpret_cast<Slot *>(&slots[i])->~Slot();
            } 
 
            free(buffer)
        }
        
        void *buffer;
        Slot *slots;
        alignas(kCacheLineSize) std::atomic<TaggedPtr> next;
        alignas(kCacheLineSize) std::atomic<uint32_t> free_next;
        alignas(kCacheLineSize) std::atomic<uint64_t> enq_idx;
        alignas(kCacheLineSize) std::atomic<uint64_t> deq_idx;
        alignas(kCacheLineSize) std::atomic<uint64_t> ref_cnt;
    };

	struct FreeList {
		void add(uint32_t new_idx) {
			TaggedPtr curr_head = head.load(std::memory_order_relaxed);
			TaggedPtr new_head = { new_idx };
			do {
				new_head.tag.store(curr_head.tag, std::memory_order_relaxed)
                nodes[new_idx].free_next.store(curr_head.idx, std::memory_order_relaxed);
			} while (!head.compare_exchange_weak(
                        curr_head,
                        new_head,
                        std::memory_order_release,
                        std::memory_order_relaxed));
		}

		Node * try_get(Node &q_tail) {
			TaggedPtr curr_head = head.load(std::memory_order_acquire);
			TaggedPtr new_head;
			
            // check for both an empty free list AND whether we need to still allocate
            while (
                    curr_head.idx != kNull && 
                    q_tail.next.load(std::memory_order_relaxed).idx == kNull) {
                
                new_head.idx = nodes[curr_head.idx].free_next.load(std::memory_order_relaxed);
				new_head.tag = curr_head.tag + 1;
				if (head.compare_exchange_weak(
                            curr_head,
                            new_head,
                            std::memory_order_release,
                            std::memory_order_acquire)) {
					break;
				}
			}
			return curr_head.idx;
		}

		std::atomic<TaggedPtr> head;
	};

private:
    size_t blocksize_; 
    alignas(kCacheLineSize) std::atomic<uint32_t> head_;
    alignas(kCacheLineSize) std::atomic<uint32_t> tail_;
};
