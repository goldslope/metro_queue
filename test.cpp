#include <cassert>
#include <iostream>
#include "metroqueue.h"

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

    MetroQueuePtr<int, true, true> spsc = MetroQueuePtr<int, true, true>(capacity, node_sz); 
    MetroQueuePtr<int, true, false> spmc = MetroQueuePtr<int, true, false>(capacity, node_sz); 
    MetroQueuePtr<int, false, true> mpsc = MetroQueuePtr<int, false, true>(capacity, node_sz); 
    MetroQueuePtr<int, false, false> mpmc = MetroQueuePtr<int, false, false>(capacity, node_sz); 

    MetroQueuePtr<int, true, false, true> spmc_nr = MetroQueuePtr<int, true, false, true>(capacity, node_sz); 
    MetroQueuePtr<int, false, false, true> mpmc_nr = MetroQueuePtr<int, false, false, true>(capacity, node_sz); 

    MetroQueuePtr<int, true, false, true, true> spmc_nr_rlx = MetroQueuePtr<int, true, false, true, true>(capacity, node_sz); 
    MetroQueuePtr<int, false, false, true, true> mpmc_nr_rlx = MetroQueuePtr<int, false, false, true, true>(capacity, node_sz); 

    MetroQueuePtr<int, false, true, false, true> mpsc_rlx = MetroQueuePtr<int, false, true, false, true>(capacity, node_sz); 
    MetroQueuePtr<int, false, false, false, true> mpmc_rlx = MetroQueuePtr<int, false, false, false, true>(capacity, node_sz); 

    test_single_thread(spsc, capacity, node_sz);
    test_single_thread(spmc, capacity, node_sz);
    test_single_thread(mpsc, capacity, node_sz);
    test_single_thread(mpmc, capacity, node_sz);
    test_single_thread(spmc_nr, capacity, node_sz);
    test_single_thread(mpmc_nr, capacity, node_sz);
    test_single_thread(spmc_nr_rlx, capacity, node_sz);
    test_single_thread(mpmc_nr_rlx, capacity, node_sz);
    test_single_thread(mpsc_rlx, capacity, node_sz);
    test_single_thread(mpmc_rlx, capacity, node_sz);

    return 0;
}
