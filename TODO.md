# TODO

## Functionality
* constructor changes
  * two args: `num_nodes`, `slots_per_node`
  * one arg: `queue_size` -> `slots_per_node * slots_per_node >= queue_size`
* portable memory pool to share between queues
  * move reset() functionality into FreeList
  * reset refcount before dealloc, maybe with new function `dealloc_reset()`
* remap indices instead of using cache line padding on Slots
  * `slot_alignment = round_up -> slot_size -> next_factor_of(interference_size)`
  * `mult = round_divide(interference_size, sizeof(Slot))`
  * `mult = round_up_power_of_2(mult)`
  * `slots_per_node = round_up_power_of_2(slots_per_node)`
  * `mapped_idx = idx * mult % slots_per_node + idx * mult / interference_size`
  * use offset on beginning AND end of Slot array, to pad from interference

## Testing
* type test (moveable, copyable)
* relacy test
* fuzz test

## Comments
* general comments
* memory synchronizayion comments
