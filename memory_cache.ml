(* Copyright (C) 2014, Thomas Leonard
 * See the README file for details. *)

(* A Hashtbl with a maximum capacity. Adding more entries when the table is full
 * deletes the least recently used entry. *)

type key = int64

type 'a node = (key * 'a) Dllist.node_t

type 'a t = {
  lru : 'a node;   (* First item is oldest *)
  tbl : (key, 'a node) Hashtbl.t;
  mutable free : int;
}

let sentinel = Obj.magic (-1L, 0)

let create max_size = {
  lru = Dllist.create sentinel;
  tbl = Hashtbl.create 10;
  free = max_size;
}

(* Move to end of LRU list *)
let mark_fresh t node =
  Dllist.remove node;
  Dllist.splice node t.lru

let lookup t key =
  try Some (Hashtbl.find t.tbl key)
  with Not_found -> None

let get t key =
  try
    let node = Hashtbl.find t.tbl key in
    mark_fresh t node;
    Some (Dllist.get node |> snd)
  with Not_found -> None

(* Drop the least recently used entry. *)
let forget_lru t =
  let node = Dllist.next t.lru in
  let key, _ = Dllist.get node in
  Dllist.remove node;
  Hashtbl.remove t.tbl key;
  t.free <- t.free + 1

let put t key value =
  let node =
    match lookup t key with
    | Some existing ->
        mark_fresh t existing;
        Dllist.set existing (key, value);
        existing
    | None ->
        if t.free = 0 then forget_lru t;
        t.free <- t.free - 1;
        Dllist.prepend t.lru (key, value)
  in
  Hashtbl.replace t.tbl key node
