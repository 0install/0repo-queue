open Mirage

let queue = foreign
  ~libraries:["fat-filesystem"; "cstruct.syntax"]
  ~packages:["fat-filesystem"; "cstruct"]
  "Unikernel.Main" (console @-> block @-> http @-> job)

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
  | `Direct -> direct_stackv4_with_dhcp console tap0
  | `Socket -> socket_stackv4 console [Ipaddr.V4.any]

let server =
  http_server 8080 (stack default_console)

let storage =
  block_of_file "disk.img"

let () =
  register "queue" [
    queue $ default_console $ storage $ server
  ]
