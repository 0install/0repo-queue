(* Copyright (C) 2014, Thomas Leonard
 * See the README file for details. *)

open Lwt

module Main (C : V1_LWT.CONSOLE) (B : V1_LWT.BLOCK) (H : Cohttp_lwt.Server) :
  sig
    val start : C.t -> B.t -> (Conduit_mirage.server -> H.t -> unit Lwt.t) -> unit Lwt.t
  end = struct
    module Q = Upload_queue.Make(B)

    let unsupported_method = H.respond_error ~status:`Bad_request ~body:"Method not supported\n" ()

    let put q request body =
      match Cohttp.Header.get request.Cohttp.Request.headers "Content-Length" with
      | None -> H.respond_error ~status:`Length_required ~body:"Missing Content-Length\n" ()
      | Some len ->
          let item = { Upload_queue.
            size = Int64.of_string len;
            data = Cohttp_lwt_body.to_stream body;
          } in
          Q.Upload.add q item >>= function
          | `Ok () -> H.respond_string ~status:`OK ~body:"Accepted\n" ()
          | `Error (`Unknown ex) -> raise ex
          | `Error `Disk_full ->
              H.respond_error ~status:`Insufficient_storage ~body:"Disk full" ()
          | `Error (`Wrong_size actual) ->
              let body = Printf.sprintf
                "Upload has wrong size: Content-Length said %Ld but got %Ld"
                item.Upload_queue.size
                actual in
              H.respond_error ~status:`Bad_request ~body ()

    let get q =
      Q.Download.peek q >>= fun {Upload_queue.size; data} ->
      let body = Cohttp_lwt_body.of_stream data in
      let headers = Cohttp.Header.init_with "Content-Length" (Int64.to_string size) in
      let encoding = Cohttp.Transfer.Fixed size in
      let res = Cohttp.Response.make ~status:`OK ~encoding ~headers () in
      return (res, body)

    let delete q =
      Q.Download.delete q >>= fun () ->
      H.respond_string ~status:`OK ~body:"OK\n" ()

    let handle_uploader q request body =
      match Cohttp.Request.meth request with
      | `PUT -> put q request body
      | `POST -> H.respond_error ~status:`Bad_request ~body:"Use PUT, not POST\n" ()
      | _ -> unsupported_method

    let handle_downloader q request =
      match Cohttp.Request.meth request with
      | `GET -> get q
      | `DELETE -> delete q
      | _ -> unsupported_method

    let start c b http =
      Log.write := C.log_s c;
      Log.info "start in queue service" >>= fun () ->

      Q.create b >>= fun q ->

      let callback _conn_id request body =
        try_lwt
          match Uri.path request.Cohttp.Request.uri with
          | "/uploader" -> handle_uploader q request body
          | "/downloader" -> handle_downloader q request
          | path -> H.respond_error ~status:`Bad_request ~body:(Printf.sprintf "Bad path '%s'\n" path) ()
        with ex ->
          Log.warn "error handling HTTP request: %s\n%s"
            (Printexc.to_string ex)
            (Printexc.get_backtrace ()) >>= fun () ->
          raise ex in

      let conn_closed _conn_id =
        Log.info "connection closed" |> ignore in

      http (`TCP 8080) (H.make ~callback ~conn_closed ())
  end
