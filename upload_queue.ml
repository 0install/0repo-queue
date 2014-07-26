(* Copyright (C) 2014, Thomas Leonard
 * See the README file for details. *)

open Lwt

type item = {
  size : int64;
  data : string Lwt_stream.t;
}

type add_error = [`Wrong_size of int64 | `Disk_full | `Unknown of exn]

let (++) = Int64.add
let (--) = Int64.sub
let (//) = Int64.div

cstruct index_entry {
  uint64 start_sector;
  uint64 length;
} as little_endian
let _ = hexdump_index_entry

module Make (B : V1_LWT.BLOCK) = struct
  type file = {
    start_sector : int64;
    file_length : int64;
  }

  type t = {
    b : B.t;
    sector_size : int64;
    total_sectors : int64;
    mutable next_free_sector : int64;
    notify : unit Lwt_condition.t;
    queue : file Queue.t;
    max_queue_length : int;
    mutable n_writers : int;
  }

  (** Abort on error *)
  let (>>|=) x f =
    x >>= function
    | `Error _ -> failwith "Block device error"
    | `Ok v -> f v

  let sectors_needed q size =
    (size ++ q.sector_size -- 1L) // q.sector_size

  let page_size = Io_page.round_to_page_size 1
  let pages_needed size =
    (size + page_size - 1) / page_size

  let get_sector_cstruct q =
    let buffer = Io_page.get (pages_needed (Int64.to_int q.sector_size)) |> Io_page.to_cstruct in
    Cstruct.sub buffer 0 (Int64.to_int q.sector_size)

  let persist_queue q =
    let l = Queue.length q.queue in
    assert (l <= q.max_queue_length);
    let buffer = get_sector_cstruct q in

    let off = ref 0 in
    let add item =
      let entry = Cstruct.sub buffer !off sizeof_index_entry in
      set_index_entry_start_sector entry item.start_sector;
      set_index_entry_length entry item.file_length;
      off := !off + sizeof_index_entry in

    q.queue |> Queue.iter add;
    let end_of_list = {start_sector = 0L; file_length = 0L} in
    for _i = l to q.max_queue_length - 1 do
      add end_of_list;
    done;

    B.write q.b 0L [buffer] >>|= return

  let restore q =
    let buffer = get_sector_cstruct q in
    B.read q.b 0L [buffer] >>|= fun () ->

    let rec load i =
      if i = q.max_queue_length then return ()
      else (
        let off = i * sizeof_index_entry in
        let entry = Cstruct.sub buffer off sizeof_index_entry in
        let item = {
          start_sector = get_index_entry_start_sector entry;
          file_length = get_index_entry_length entry;
        } in
        if item.start_sector = 0L then return ()
        else if (item.start_sector < 1L ||
                 item.file_length < 0L ||
                 item.start_sector ++ sectors_needed q item.file_length > q.total_sectors) then (
          Log.info "Discarding invalid item at %Ld, length %Ld" item.start_sector item.file_length
        ) else (
          Log.info "Restored item at %Ld, length %Ld" item.start_sector item.file_length >>= fun () ->
          let next_free = item.start_sector ++ (sectors_needed q item.file_length) in
          if next_free > q.next_free_sector then q.next_free_sector <- next_free;
          Queue.add item q.queue;
          load (i + 1)
        )
      ) in
    load 0

  let create b =
    B.get_info b >>= fun info ->
    let max_queue_length = info.B.sector_size / sizeof_index_entry in
    let q = {
      b;
      sector_size = Int64.of_int info.B.sector_size;
      total_sectors = info.B.size_sectors;
      next_free_sector = 1L;
      notify = Lwt_condition.create ();
      queue = Queue.create ();
      max_queue_length;
      n_writers = 0;
    } in
    restore q >>= fun () ->
    return q

  let maybe_reset q =
    if Queue.is_empty q.queue && q.n_writers = 0 then (
      q.next_free_sector <- 1L;
      Log.info "Queue is empty and idle; resetting"
    ) else return ()

  module Upload = struct
    exception Wrong_size of int64
    exception Disk_full

    let add_as q {size; data} =
      let start_sector = q.next_free_sector in
      let next_file = start_sector ++ sectors_needed q size in
      if next_file > q.total_sectors then raise Disk_full;
      q.next_free_sector <- next_file;

      (* Stream data to file. *)
      let file_offset = ref 0L in

      let raw_buffer = Io_page.get 256 in
      let pages = Io_page.to_pages raw_buffer |> List.map Io_page.to_cstruct |> Array.of_list in
      let page_buffer = Io_page.to_cstruct raw_buffer in
      let page_buffer_used = ref 0 in

      let flush_page_buffer () =
        Log.info "flushing %d bytes to disk" !page_buffer_used >>= fun () ->
        let buffered_data = Array.sub pages 0 (pages_needed !page_buffer_used) |> Array.to_list in
        let next_offset = !file_offset ++ Int64.of_int !page_buffer_used in
        if next_offset > size then raise (Wrong_size next_offset);
        if Int64.rem !file_offset q.sector_size <> 0L then failwith "Non-aligned write!";
        let sector = start_sector ++ (!file_offset // q.sector_size) in
        B.write q.b sector buffered_data >>|= fun () ->
        file_offset := next_offset;
        page_buffer_used := 0;
        return () in

      let rec add_data src i =
        let src_remaining = String.length src - i in
        if src_remaining = 0 then return ()
        else (
          let page_buffer_free = Cstruct.len page_buffer - !page_buffer_used in
          let chunk_size = min page_buffer_free src_remaining in
          Cstruct.blit_from_string src i page_buffer !page_buffer_used chunk_size;
          page_buffer_used := !page_buffer_used + chunk_size;
          lwt () = if page_buffer_free = chunk_size then flush_page_buffer () else return () in
          add_data src (i + chunk_size)
        ) in

      data |> Lwt_stream.iter_s (fun data -> add_data data 0) >>=
      flush_page_buffer >>= fun () ->

      (* Check size is correct and flag file as complete. *)
      let actual_size = !file_offset in
      if actual_size = size then (
        Log.info "added file of size %Ld bytes" size >>= fun () ->
        if Queue.length q.queue >= q.max_queue_length then raise Disk_full;
        Queue.add {start_sector; file_length = size} q.queue;
        persist_queue q >>= fun () ->
        Lwt_condition.broadcast q.notify ();
        return (`Ok ())
      ) else (
        raise (Wrong_size actual_size)
      )

    let add q item =
      try_lwt
        q.n_writers <- q.n_writers + 1;
        Lwt.finalize
          (fun () -> add_as q item)
          (fun () -> q.n_writers <- q.n_writers - 1; maybe_reset q)
      with
      | Disk_full ->
          Log.info "disk full" >>= fun () ->
          return (`Error `Disk_full)
      | Wrong_size actual ->
          Log.info "wrong size: expected %Ld, got %Ld" item.size actual >>= fun () ->
          return (`Error (`Wrong_size actual))
      | ex ->
          Log.warn "failed to store upload: %s" (Printexc.to_string ex) >>= fun () ->
          return (`Error (`Unknown ex))
  end

  module Download = struct
    (** This interface is not safe with multiple clients, but add some minimal protection anyway. *)
    let mutex = Lwt_mutex.create ()

    let being_processed = ref None
    let pages_per_chunk = 16
    let max_chunk_size = Int64.of_int (pages_per_chunk * page_size)

    let send q item =
      let offset = ref 0L in
      let next () =
        try_lwt
          if Some item <> !being_processed then failwith "Item deleted during read!";
          let chunk_size = min max_chunk_size (item.file_length -- !offset) in
          if chunk_size = 0L then return None
          else (
            let sector = item.start_sector ++ (!offset // q.sector_size) in
            let bufs = Io_page.pages (pages_needed (Int64.to_int chunk_size)) |> List.map Io_page.to_cstruct in
            B.read q.b sector bufs >>|= fun () ->
            offset := !offset ++ chunk_size;
            let data = String.sub (Cstruct.copyv bufs) 0 (Int64.to_int chunk_size) in
            (* Log.info "Sending chunk '%s'" data >>= fun () -> *)
            return (Some data)
          )
        with ex ->
          Log.info "read failed: %s" (Printexc.to_string ex) >>= fun () ->
          return None in
      return {
        size = item.file_length;
        data = Lwt_stream.from next
      }

    let peek q =
      Lwt_mutex.with_lock mutex (fun () ->
        match !being_processed with
        | Some item ->
            Log.info "resending head item to client" >>= fun () ->
            send q item      (* Maybe the client didn't get it; send it again *)
        | None ->
            Log.info "taking item from queue..." >>= fun () ->
            let rec loop () =
              let item =
                try Some (Queue.peek q.queue)
                with Queue.Empty -> None in
              match item with
              | None -> Lwt_condition.wait q.notify >>= loop
              | Some item ->
                  Log.info "returning item" >>= fun () ->
                  being_processed := Some item;
                  send q item
            in
            loop ()
      )

    let delete q =
      Lwt_mutex.with_lock mutex (fun () ->
        if !being_processed = None then (
          (* If the client wasn't sure we got their delete, they may retry. *)
          Log.info "DELETE called but nothing pending"
        ) else (
          Log.info "deleting head item" >>= fun () ->
          Queue.pop q.queue |> ignore;
          persist_queue q >>= fun () ->
          being_processed := None;
          maybe_reset q
        )
      )
  end
end
