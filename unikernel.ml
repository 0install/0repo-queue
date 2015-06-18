(* Copyright (C) 2014, Thomas Leonard
 * See the README file for details. *)

open Lwt

let (>>|=) x f =
  x >>= function
  | `Error (`Unknown msg) -> failwith msg
  | `Error _ -> failwith "Error"
  | `Ok v -> f v

let (--) = Int64.sub

let private_key_len = 887
let key_partition_size = 4096

let round_up bytes block_size =
  let blocks = (bytes + block_size - 1) / block_size in
  blocks * block_size

module Main (C : V1_LWT.CONSOLE) (B : V1_LWT.BLOCK) (H : Cohttp_lwt.Server) (CertStore : V1_LWT.KV_RO) =
  struct
    module Part = Mbr_partition.Make(B)
    module Q = Upload_queue.Make(Part)

    let read_from_partition b ~len =
      Part.get_info b >>= fun info ->
      let buf_size = round_up len info.Part.sector_size in
      let pages_needed = Io_page.round_to_page_size buf_size / Io_page.round_to_page_size 1 in
      let page_buf = Io_page.get pages_needed |> Io_page.to_cstruct in
      let buf = Cstruct.sub page_buf 0 buf_size in
      Part.read b 0L [buf] >>|= fun () ->
      return (Cstruct.sub buf 0 len)

    let read_from_kv kv name =
      let (>>|=) x f =
        x >>= function
        | `Ok x -> f x
        | `Error (CertStore.Unknown_key _) -> failwith name in
      CertStore.size kv name >>|= fun len ->
      CertStore.read kv name 0 (Int64.to_int len) >>|= fun bufs ->
      return (Cstruct.copyv bufs |> Cstruct.of_string)

    let unsupported_method = H.respond_error ~status:`Bad_request ~body:"Method not supported\n" ()

    let put q request body ~user =
      match Cohttp.Header.get request.Cohttp.Request.headers "Content-Length" with
      | None -> H.respond_error ~status:`Length_required ~body:"Missing Content-Length\n" ()
      | Some len ->
          let item = { Upload_queue.
            size = Int64.of_string len;
            data = Cohttp_lwt_body.to_stream body;
          } in
          Log.info "%Ld byte item uploaded by %s" item.Upload_queue.size user >>= fun () ->
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

    let handle_uploader q request body ~user =
      match Cohttp.Request.meth request with
      | `PUT -> put q request body ~user
      | `POST -> H.respond_error ~status:`Bad_request ~body:"Use PUT, not POST\n" ()
      | _ -> unsupported_method

    let handle_downloader q request =
      match Cohttp.Request.meth request with
      | `GET -> get q
      | `DELETE -> delete q
      | _ -> unsupported_method

    let bad_path path =
      H.respond_error ~status:`Bad_request ~body:(Printf.sprintf "Bad path '%s'\n" path) ()

    let re_endpoint = Str.regexp "^/\\(uploader\\|downloader\\)/!\\(.*\\)$"

    let callback q _conn_id request body =
      try_lwt
        let path = Uri.path request.Cohttp.Request.uri in
        if Str.string_match re_endpoint path 0 then (
          let label = Str.matched_group 1 path in
          let swiss_hash = Str.matched_group 2 path
            |> Cstruct.of_string
            |> Nocrypto.Hash.digest `SHA256
            |> Cstruct.to_string
            |> B64.encode in
          match label, swiss_hash with
          | "uploader", "kW8VOKYP1eu/cWInpJx/jzYDSJzo1RUR14GoxV/CImM=" -> handle_uploader q request body ~user:"Alice"
          | "downloader", "PEj3nuboy3BktVGzi9y/UBmgkrGuhHD1i6WsXDw1naI=" -> handle_downloader q request
          | _ -> bad_path path
        ) else bad_path path
      with ex ->
        Log.warn "error handling HTTP request: %s\n%s"
          (Printexc.to_string ex)
          (Printexc.get_backtrace ()) >>= fun () ->
        raise ex

    let conn_closed _conn_id =
      Log.info "connection closed" |> ignore

    let start c b http cert_store =
      Log.write := C.log_s c;
      Log.info "start in queue service" >>= fun () ->

      B.get_info b >>= fun info ->
      let key_sectors = (key_partition_size + info.B.sector_size - 1) / info.B.sector_size |> Int64.of_int in
      let queue_sectors = info.B.size_sectors -- key_sectors in

      Part.(connect {b; start_sector = 0L; length_sectors = key_sectors}) >>|= fun key_partition ->
      Part.(connect {b; start_sector = key_sectors; length_sectors = queue_sectors}) >>|= fun queue_partition ->

      Q.create queue_partition >>= fun q ->

      lwt certs = read_from_kv cert_store "tls/server.pem" >|= X509.Encoding.Pem.Cert.of_pem_cstruct
      and pk = read_from_partition key_partition ~len:private_key_len >|= X509.Encoding.Pem.PK.of_pem_cstruct1 in
      let certificates = `Single (certs, pk) in
      let tls_config = Tls.Config.server ~certificates () in
      http (`TLS (tls_config, `TCP 8443)) (H.make ~callback:(callback q) ~conn_closed ())
  end
