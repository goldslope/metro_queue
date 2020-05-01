#include <cassert>
#include <iostream>
#include "metro_queue.h"

template <typename QueuePtr>
void test_single_thread(QueuePtr& q, size_t capacity, size_t node_sz) {
    auto og_capacity = capacity;
    for (int j = 0; j < 100; ++j) {
        if (j >= 1) {
            capacity = og_capacity - node_sz;
        }

        // pop init test
        for (int i = 0; i < capacity; ++i) {
            int b = i;
            assert(!q->try_pop(b));
            assert(b == i);
        }

        // push success test
        for (int i = 0; i < capacity; ++i) {
            assert(q->try_push(i));
        }

        // pop fail test
        for (int i = 0; i < capacity; ++i) {
            assert(!q->try_push(i));
        }

        // pop success test
        for (int i = 0; i < capacity; ++i) {
            int b = 0;
            assert(q->try_pop(b));
            assert(b == i);
        }

        // pop fail test
        for (int i = 0; i < capacity; ++i) {
            int b = 0;
            assert(!q->try_pop(b));
            assert(b == 0);
        }

        // interleave test
        for (int i = 0; i < capacity * 100; ++i) {
            int b = 0;
            assert(q->try_push(i));
            assert(q->try_pop(b));
            assert(b == i);
            assert(!q->try_pop(b));
        }
    }
}

int main() {
    auto capacity = 20;
    auto node_sz = 5;

    metro::SPSCQueuePtr<int> spsc = metro::SPSCQueuePtr<int>(capacity, node_sz);
    metro::SPMCQueuePtr<int> spmc = metro::SPMCQueuePtr<int>(capacity, node_sz);
    metro::MPSCQueuePtr<int> mpsc = metro::MPSCQueuePtr<int>(capacity, node_sz);
    metro::MPMCQueuePtr<int> mpmc = metro::MPMCQueuePtr<int>(capacity, node_sz);

    metro::SPMCRefQueuePtr<int> spmc_nr = metro::SPMCRefQueuePtr<int>(capacity, node_sz);
    metro::MPMCRefQueuePtr<int> mpmc_nr = metro::MPMCRefQueuePtr<int>(capacity, node_sz);

    std::cout << "Running SPSC test..." << std::endl;
    test_single_thread(spsc, capacity, node_sz);
    std::cout << "PASSED!" << std::endl;

    std::cout << "Running SPMC test..." << std::endl;
    test_single_thread(spmc, capacity, node_sz);
    std::cout << "PASSED!" << std::endl;

    std::cout << "Running MPSC test..." << std::endl;
    test_single_thread(mpsc, capacity, node_sz);
    std::cout << "PASSED!" << std::endl;

    std::cout << "Running MPMC test..." << std::endl;
    test_single_thread(mpmc, capacity, node_sz);
    std::cout << "PASSED!" << std::endl;

    std::cout << "Running SPMC (w NodeRef) test..." << std::endl;
    test_single_thread(spmc_nr, capacity, node_sz);
    std::cout << "PASSED!" << std::endl;

    std::cout << "Running MPMC (w NodeRef) test..." << std::endl;
    test_single_thread(mpmc_nr, capacity, node_sz);
    std::cout << "PASSED!" << std::endl;

    return 0;
}
