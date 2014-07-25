module Make : functor (B : V1_LWT.BLOCK) ->
  V1_LWT.BLOCK with
  type id = B.t
