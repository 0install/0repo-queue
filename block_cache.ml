(* Copyright (C) 2014, Thomas Leonard
 * See the README file for details. *)

open Lwt

module Make(B : V1_LWT.BLOCK) = struct
  let string_of_error = function
    | `Unknown x -> x
    | `Unimplemented -> "Unimplemented"
    | `Is_read_only -> "Is_read_only"
    | `Disconnected -> "Disconnected"

  (** Abort on error *)
  let (>>|=) x f =
    x >>= function
    | `Error x -> failwith (string_of_error x)
    | `Ok v -> f v

  type t = {
    raw : B.t;
  }

  let write cache sector_start buffers =
    B.write cache.raw sector_start buffers

  let read cache sector_start buffers =
    B.read cache.raw sector_start buffers

  let disconnect cache = B.disconnect cache.raw

  type id = B.t

  (* Why can't this just be B.info? *)
  type info = {
    read_write : bool;
    sector_size : int;
    size_sectors : int64;
  }

  let get_info cache =
    B.get_info cache.raw >>= fun raw_info ->
    return {
      read_write = raw_info.B.read_write;
      sector_size = raw_info.B.sector_size;
      size_sectors = raw_info.B.size_sectors;
    }

  type 'a io = 'a B.io
  type error = B.error

  let id cache = cache.raw

  type page_aligned_buffer = B.page_aligned_buffer

  let connect raw =
    return (`Ok { raw })
end
