(* Copyright (C) 2014, Thomas Leonard
 * See the README file for details. *)

open Lwt

module type FS = V1_LWT.FS with
  type page_aligned_buffer = Cstruct.t and
  type block_device_error = Fat.Fs.block_error

type filepath = string
type item = {
  size : int64;
  data : string Lwt_stream.t;
}

type add_error = [`Wrong_size of int64 | `Unknown of exn]

module Make (F : FS) = struct
  type t = {
    fs : F.t;
    queue : filepath Lwt_stream.t;
    push : filepath -> unit Lwt.t
  }

  (** Abort on error *)
  let (>>|=) x f =
    x >>= function
    | `Error x -> failwith (Fat.Fs.string_of_filesystem_error x)
    | `Ok v -> f v

  let create fs =
    let queue, push_opt = Lwt_stream.create () in
    let push name =
      Log.info "adding '%s' to queue" name >>= fun () ->
      push_opt (Some name);
      return () in

    (* On start-up, check for left-over files.
     * If complete, add them to the queue. Otherwise, delete them. *)
    let restore leaf =
      let name = "/" ^ leaf in
      F.read fs name 0 1 >>|= fun bufs ->
      match Cstruct.copyv bufs with
      | "N" | "" ->
          Log.info "discarding incomplete upload '%s'" name >>= fun () ->
          F.destroy fs name >>|= return
      | "Y" ->
          push name
      | f ->
          failwith (Printf.sprintf "Invalid flag '%s'" f) in

    (* Note: not necessarily in the original order... *)
    F.listdir fs "/" >>|=
    Lwt_list.iter_s restore >>= fun () ->

    return { fs; queue; push }

  module Upload = struct
    let i = ref 0
    let rec mktemp fs =
      incr i;
      let name = "/item-" ^ string_of_int !i in
      F.create fs name >>= function
      | `Ok () ->
          Log.info "created new file '%s'" name >> return name
      | `Error (`File_already_exists _) ->
          Log.info "file '%s' exists; retrying..." name >> mktemp fs
      | `Error x -> failwith (Fat.Fs.string_of_filesystem_error x)

    let add_as q name {size; data} =
      (* Set the first byte to N to indicate that we're not done yet.
       * If we reboot while this flag is set, the partial upload will
       * be deleted. *)
      let partial_flag = Cstruct.of_string "N" in
      F.write q.fs name 0 partial_flag >>|= fun () ->

      (* Stream data to file. *)
      let file_offset = ref 1 in

      let page_buffer = Io_page.get 256 |> Io_page.to_cstruct in
      let page_buffer_used = ref 0 in

      let flush_page_buffer () =
        Log.info "Flushing %d bytes to disk" !page_buffer_used >>= fun () ->
        let buffered_data = Cstruct.sub page_buffer 0 !page_buffer_used in
        F.write q.fs name !file_offset buffered_data >>|= fun () ->
        file_offset := !file_offset + !page_buffer_used;
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
      let actual_size = Int64.of_int (!file_offset - 1) in
      if actual_size = size then (
        let complete_flag = Cstruct.of_string "Y" in
        F.write q.fs name 0 complete_flag >>|= fun () ->
        q.push name >> return (`Ok ())
      ) else (
        return (`Error (`Wrong_size actual_size))
      )

    let add q item =
      mktemp q.fs >>= fun name ->

      begin try_lwt
        add_as q name item
      with ex ->
        return (`Error (`Unknown ex))
      end >>= function
      | `Ok () as ok -> return ok
      | `Error _ as err ->
          Log.info "rm '%s'" name >>= fun () ->
          F.destroy q.fs name >>|= fun () ->
          return err
  end

  module Download = struct
    (** This interface is safe with multiple clients, but add some minimal protection anyway. *)
    let mutex = Lwt_mutex.create ()

    let being_processed = ref None

    let send q name =
      F.size q.fs name >>|= fun len ->
      let offset = ref 1L in
      let max_chunk_size = 0x100000L in
      let next () =
        try_lwt
          let chunk_size = min max_chunk_size (Int64.sub len !offset) in
          if chunk_size = 0L then return None
          else (
            F.read q.fs name (Int64.to_int !offset) (Int64.to_int chunk_size) >>|= fun bufs ->
            offset := Int64.add !offset chunk_size;
            let data = Cstruct.copyv bufs in
            (* Log.info "Sending chunk '%s'" data >>= fun () -> *)
            return (Some data)
          )
        with ex ->
          Log.info "read of '%s' failed: %s" name (Printexc.to_string ex) >>= fun () ->
          return None in
      return {
        size = Int64.sub len 1L;
        data = Lwt_stream.from next
      }

    let peek q =
      Lwt_mutex.with_lock mutex (fun () ->
        match !being_processed with
        | Some name ->
            Log.info "resending head item (%s) to client" name >>
            send q name       (* Maybe the client didn't get it; send it again *)
        | None ->
            Log.info "taking item from queue..." >>
            Lwt_stream.next q.queue >>= fun name ->
            Log.info "returning item '%s'" name >>= fun () ->
            being_processed := Some name;
            send q name
      )

    let delete q =
      Lwt_mutex.with_lock mutex (fun () ->
        match !being_processed with
        | None ->
            (* If the client wasn't sure we got their delete, they may retry. *)
            Log.info "DELETE called but nothing pending"
        | Some name ->
            Log.info "rm '%s'" name >>
            F.destroy q.fs name >>|= fun () ->
            being_processed := None;
            return ()
      )
  end
end
