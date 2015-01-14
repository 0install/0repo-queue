open Mirage

let queue = foreign
  ~libraries:["cstruct.syntax";"mirage-http";"x509";"mbr-format.mirage"]
  ~packages:["cstruct";"mirage-http";"x509";"mbr-format"]
  "Unikernel.Main" (console @-> block @-> conduit @-> kv_ro @-> job)

let net =
  match get_mode () with
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

let conduit =
  conduit_direct ~tls:(tls_over_conduit default_entropy) (stack default_console)

let storage =
  match get_mode () with
  | `Xen -> block_of_file "xvda"
  | `Unix -> block_of_file "disk.img"

let () =
  register "queue" [
    queue $ default_console $ storage $ conduit $ crunch "keys"
  ]
