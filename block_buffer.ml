(* Copyright (C) 2014, Thomas Leonard
 * See the README file for details. *)

(** Does read-ahead on block devices to improve performance. *)

open Lwt
let (++) = Int64.add
let (--) = Int64.sub

module Make (B : V1_LWT.BLOCK) = struct
  type t = {
    b : B.t;
    sectors_per_read : int;
    mutable next : int64;
    limit : int64;
    q : Cstruct.t Lwt.t Queue.t;
    qlen : int;
    sector_size : int;
  }

  let page_size = Io_page.round_to_page_size 1

  let fill t =
    while Queue.length t.q < t.qlen && t.next < t.limit do
      let sectors_remaining = t.limit -- t.next in
      let sectors_to_get = min sectors_remaining (Int64.of_int t.sectors_per_read) |> Int64.to_int in
(*         Printf.printf "Reading %d sectors from %Ld\n%!" sectors_to_get t.next; *)
      let pages_needed = Io_page.round_to_page_size (sectors_to_get * t.sector_size) / page_size in
      let pages = Io_page.get pages_needed in
      let buffer = Io_page.to_pages pages |> List.map Io_page.to_cstruct in
      let result_buffer = Cstruct.sub (Io_page.to_cstruct pages) 0 (sectors_to_get * t.sector_size) in
      let result =
        B.read t.b t.next buffer >>= function
        | `Ok () -> return result_buffer
        | `Error _ -> failwith "Block.read failed!" in
      Queue.add result t.q;
      t.next <- t.next ++ Int64.of_int sectors_to_get
    done

  let create b ~qlen ~sectors_per_read ~start ~len =
    B.get_info b >>= fun info ->
    return {
      b;
      qlen;
      sectors_per_read;
      next = start;
      limit = start ++ len;
      q = Queue.create ();
      sector_size = info.B.sector_size;
    }

  let read t =
    fill t;
    try Some (Queue.pop t.q)
    with Queue.Empty -> None
end
