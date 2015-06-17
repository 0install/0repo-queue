open Mirage

let mode =
  match get_mode () with
  | `Xen | `Unix as m -> m
  | _ -> failwith "Unsupported mode"

let queue = foreign
  ~libraries:["cstruct.syntax"]
  ~packages:["cstruct"]
  "Unikernel.Main" (console @-> block @-> http @-> job)

let net =
  match mode with
  | `Xen -> `Direct
  | `Unix ->
      try match Sys.getenv "NET" with
        | "direct" -> `Direct
        | "socket" -> `Socket
        | _        -> `Direct
      with Not_found -> `Socket

let stack console =
  match net with
  | `Direct -> direct_stackv4_with_default_ipv4 console tap0
  | `Socket -> socket_stackv4 console [Ipaddr.V4.any]

let server =
  http_server (conduit_direct (stack default_console))

let storage =
  block_of_file "xvda"

let () =
  register "queue" [
    queue $ default_console $ storage $ server
  ]
