(* Copyright (C) 2014, Thomas Leonard
 * See the README file for details. *)

open Lwt

module A = Bigarray.Array1

type +'a io = 'a Lwt.t
type t = Cstruct.t
type id = unit
type error = [
  | `Unknown of string (** an undiagnosed error *)
  | `Unimplemented     (** operation not yet implemented in the code *)
  | `Is_read_only      (** you cannot write to a read/only instance *)
  | `Disconnected      (** the device has been previously disconnected *)
]

let sector_size = 512

type info = {
  read_write: bool;    (** True if we can write, false if read/only *)
  sector_size: int;    (** Octets per sector *)
  size_sectors: int64; (** Total sectors per device *)
}

let write device sector_start buffers =
  let rec loop dstoff = function
    | [] -> ()
    | x :: xs ->
        Cstruct.blit x 0 device dstoff (Cstruct.len x);
        loop (dstoff + (Cstruct.len x)) xs in
  loop (Int64.to_int sector_start * sector_size) buffers;
  `Ok () |> return

let read device sector_start buffers =
  let rec loop dstoff = function
    | [] -> ()
    | x :: xs ->
        Cstruct.blit device dstoff x 0 (Cstruct.len x);
        loop (dstoff + (Cstruct.len x)) xs in
  loop (Int64.to_int sector_start * sector_size) buffers;
  `Ok () |> return

let info = {
  read_write = true;
  sector_size;
  size_sectors = 64L;
}

let size = info.sector_size * Int64.to_int info.size_sectors

let get_info _device = return info

let disconnect _device = return ()

let id _device = ()

type page_aligned_buffer = Cstruct.t

let connect () =
  let data = Cstruct.create size in
  `Ok data |> return
