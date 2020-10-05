# Metro Queue (Work In Progress)

## Testing Phase
Currently passes basic single-threaded queue correctness tests.

## What is it?
Metro Queue is an almost lock-free queue (see **Progress Guarantees**).
By default, it is a multi-producer, multi-consumer queue. However,
it can optimized for single-producer and/or single-consumer cases
via template parameters.

## How does it work?
The underlying structure is a linked-list of nodes, each of which
contain an array of slots. This is where the name "Metro" comes from.
The nodes are like train cars, linked to eachother, and each node can
contain multiple items, just as a train car can carry multiple people.
There are other queues that work in a similar manner, but these
queues usually require something like hazard pointers to avoid the ABA
problem. This queue instead uses pointer tagging and states to ensure
that ABA data is always invalidated. This removes the need for any
sequentially consistent operations or other hazard-pointer-like
methods to prevent the ABA problem. What's the catch? The catch is
that nodes cannot be reclaimed to the operating system; instead, they
must deallocated to a local free-list which is managed by the queue.

## Features
* works with any object that is exception-safe move or copy assignable 
* template specialized SPSC, SPMC, MPSC, MPMC queues
* bulk push and pop operations
* no thread registration

## Fast-path Operations (MPMC -- roughly, for large node and small object)
* single push: 1 contested fetch-add + 1 uncontested compare-exchange, node operations amortized 
* single pop: 1 contested fetch-add + 1 uncontested shared read, node operations amortized
* bulk N push: 1 contested fetch-add + N uncontested compare-exchange's, node operations amortized
* bulk N pop: 1 contested fetch-add + N uncontested shared reads, node operations amortized

## Progress Guarantees (MPMC)
* Lockfree Producers
* Blocking Consumers
  * a producer may block only one consumer, if producer gets descheduled/dies in the middle of push
  * remaining producers and consumers are unaffected
  * one node will also be blocked from being freed
  * if NodeRef's aren't used, whichever thread is responsible for freeing the node, will be blocked as well
  * summary (w NodeRef's): 1 Producer BLOCKS 1 Consumer
  * summary (w/o NodeRef's): 1 Producer BLOCKS 1 Consumer BLOCKS 1 Deallocator (Producer or Consumer)
