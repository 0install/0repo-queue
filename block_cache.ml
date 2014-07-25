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
    cache : B.page_aligned_buffer Memory_cache.t;
    sector_len : int;
    mem_cache_size : int;
  }

  (** Call [fn sector page] for each page in each buffer. *)
  let each_page bc sector_start buffers fn =
    let do_buffer sector buffer =
      let len = Cstruct.len buffer in
      let rec loop_page s i =
        if i = len then return ()
        else (
          let page = Cstruct.sub buffer i bc.sector_len in
          fn s page >>= fun () ->
          loop_page (Int64.add s 1L) (i + bc.sector_len)
        ) in
      loop_page sector 0 in

    let rec loop s = function
      | [] -> return (`Ok ())
      | b :: bs ->
          do_buffer s b >>= fun () ->
          loop (Int64.add s (Cstruct.len b / bc.sector_len |> Int64.of_int)) bs
    in
    loop sector_start buffers

  let mutex = Lwt_mutex.create ()

  let write bc sector_start buffers =
    Lwt_mutex.with_lock mutex (fun () ->
      B.write bc.raw sector_start buffers >>|= fun () ->
      each_page bc sector_start buffers (fun sector page ->
        let cached = Cstruct.create bc.sector_len in
        Cstruct.blit page 0 cached 0 bc.sector_len;
        Memory_cache.put bc.cache sector cached;
        return ()
      )
    )

  let read bc sector_start buffers =
    Lwt_mutex.with_lock mutex (fun () ->
      each_page bc sector_start buffers (fun sector page ->
        match Memory_cache.get bc.cache sector with
        | Some cached ->
            Cstruct.blit cached 0 page 0 bc.sector_len;
            return ()
        | None ->
            B.read bc.raw sector [page] >>|= fun () ->
            let cached = Cstruct.create bc.sector_len in
            Cstruct.blit page 0 cached 0 bc.sector_len;
            Memory_cache.put bc.cache sector cached;
            return ()
      )
    )

  let disconnect cache = B.disconnect cache.raw

  type id = B.t * int

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

  let id cache = (cache.raw, cache.mem_cache_size)

  type page_aligned_buffer = B.page_aligned_buffer

  let connect (raw, size) =
    B.get_info raw >>= fun info ->
    return (`Ok {
      sector_len = info.B.sector_size;
      raw;
      cache = Memory_cache.create (size / info.B.sector_size);
      mem_cache_size = size;
    })
end
