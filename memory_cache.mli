(* Copyright (C) 2014, Thomas Leonard
 * See the README file for details. *)

(* A Hashtbl with a maximum capacity. Adding more entries when the table is full
 * deletes the least recently used entry. *)

type 'a t

(* [create max] creates a cache which can store up to [max] items. *)
val create : int -> 'a t

val get : 'a t -> int64 -> 'a option

val put : 'a t -> int64 -> 'a -> unit
