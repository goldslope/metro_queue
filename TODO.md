# TODO

## Functionality
* portable memory pool to share between queues
* remap indices instead of using cache line padding on Slots
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
