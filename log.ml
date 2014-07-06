(* Copyright (C) 2014, Thomas Leonard
 * See the README file for details. *)

let write = ref (fun x -> print_endline x; Lwt.return ())

let info fmt = Printf.ksprintf !write ("info: " ^^ fmt)
let warn fmt = Printf.ksprintf !write ("WARN: " ^^ fmt)
