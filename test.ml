(* Copyright (C) 2014, Thomas Leonard
 * See the README file for details. *)

open OUnit
open Lwt

module Q = Upload_queue.Make(Fake_block)

(* Remove this to get verbose output *)
let () =
  Log.write := (fun _ -> Lwt.return ())

let (>>|=) x f =
  x >>= function
  | `Ok y -> f y
  | `Error _ -> assert false

let prefix x = String.sub x 0 (min 10 (String.length x))

let make_queue () =
  Fake_block.connect () >>|= fun b ->
  Q.create b

let read {Upload_queue.size; data} =
  Lwt_stream.to_list data >>= fun items ->
  let contents = String.concat "" items in
  assert_equal ~printer:Int64.to_string size (String.length contents |> Int64.of_int);
  return contents

let check_download q expected =
  Q.Download.peek q >>= read >>= fun contents ->
  assert_equal ~printer:prefix expected contents;
  Q.Download.delete q

let with_queue fn : unit =
  Lwt_unix.run (make_queue () >>= fn)

let suite = "queue" >:::[

  "full" >:: (fun () -> with_queue (fun q ->
    let upload x =
      let data = string_of_int x ^ String.make 5370 ' ' in
      let item = { Upload_queue.
        size = String.length data |> Int64.of_int;
        data = Lwt_stream.of_list [data];
      } in
      Q.Upload.add q item in
    for_lwt x = 1 to 5 do upload x >>|= return done >>= fun () ->
    upload 6 >>= function
    | `Ok () | `Error (`Wrong_size _ | `Unknown _) -> assert false
    | `Error `Disk_full ->

    for_lwt x = 1 to 5 do
      let expected = string_of_int x ^ String.make 5370 ' ' in
      check_download q expected
    done >>= fun () ->

    upload 1 >>|= return
  ));

  "parallel" >:: (fun () -> with_queue (fun q ->
    let stream1, push1 = Lwt_stream.create () in
    let stream2, push2 = Lwt_stream.create () in

    Q.Upload.add q { Upload_queue.size = 5L; data = stream1 } |> ignore;
    Q.Upload.add q { Upload_queue.size = 6L; data = stream2 } |> ignore;

    (* We start uploading stream1 first, but stream2 is the first to finish. *)
    push1 (Some "First");
    push2 (Some "Second");
    push2 None;

    check_download q "Second" >>= fun () ->

    let first = Q.Download.peek q in
    push1 None;
    first >>= read >>= fun content1 ->
    assert_equal "First" content1;
    Q.Download.delete q
  ));

  "wrong-size" >:: (fun () -> with_queue (fun q ->
    Q.Upload.add q { Upload_queue.size = 5L; data = Lwt_stream.of_list ["he"; "ll"] } >>= begin function
    | `Error (`Wrong_size 4L) -> return ()
    | _ -> assert false end >>= fun () ->

    Q.Upload.add q { Upload_queue.size = 5L; data = Lwt_stream.of_list ["he"; "llo!"] } >>= begin function
    | `Error (`Wrong_size 6L) -> return ()
    | _ -> assert false end >>= fun () ->

    Q.Upload.add q { Upload_queue.size = 5L; data = Lwt_stream.of_list ["he"; "llo"] } >>|= fun () ->

    check_download q "hello"
  ));

  "crash" >:: (fun () -> Lwt_unix.run begin
    Fake_block.connect () >>|= fun b ->
    Q.create b >>= fun q ->

    let stream1, push1 = Lwt_stream.create () in
    let stream2, push2 = Lwt_stream.create () in

    Q.Upload.add q { Upload_queue.size = 5L; data = stream1 } |> ignore;
    Q.Upload.add q { Upload_queue.size = 6L; data = stream2 } |> ignore;

    (* We start uploading stream1 first, but only stream2 finishes. *)
    push1 (Some "First");
    push2 (Some "Second");
    push2 None;

    (* Crash, reboot... *)
    Q.create b >>= fun q ->

    Q.Upload.add q { Upload_queue.size = 5L; data = Lwt_stream.of_list ["hello"] } >>|= fun () ->

    check_download q "Second" >>= fun () ->
    check_download q "hello"
  end);

  ]

let () =
  Printexc.record_backtrace true;
  suite |> run_test_tt_main |> ignore;
  Format.print_newline ()
