module Make : functor (B : V1_LWT.BLOCK) ->
  V1_LWT.BLOCK with
  type id = (B.t * int)    (* Underlying block device and RAM allocation for cache *)
