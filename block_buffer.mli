(* Copyright (C) 2014, Thomas Leonard
 * See the README file for details. *)

(** Does read-ahead on block devices to improve performance. *)

module Make (B : V1_LWT.BLOCK) : sig
  type t
  val create : B.t -> qlen:int -> sectors_per_read:int -> start:int64 -> len:int64 -> t Lwt.t
  val read : t -> Cstruct.t Lwt.t option
end
