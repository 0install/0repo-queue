open Mirage

let mode =
  match get_mode () with
  | `Xen | `Unix as m -> m
  | _ -> failwith "Unsupported mode"

let queue = foreign
  ~libraries:["cstruct.syntax";"x509";"mbr-format.mirage"]
  ~packages:["cstruct";"x509";"mbr-format"]
  "Unikernel.Main" (console @-> block @-> http @-> kv_ro @-> job)

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
  http_server (conduit_direct ~tls:true (stack default_console))

let storage =
  match mode with
  | `Xen -> block_of_file "xvda"
  | `Unix -> block_of_file "disk.img"

let () =
  register "queue" [
    queue $ default_console $ storage $ server $ crunch "keys"
  ]
