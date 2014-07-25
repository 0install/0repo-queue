(* Copyright (C) 2014, Thomas Leonard
 * See the README file for details. *)

open OUnit
open Lwt

module BC = Block_cache.Make(Fake_block)
module Fat1 = Fat.Fs.Make(BC)(Io_page)
module Q = Upload_queue.Make(Fat1)

let () =
  (* Remove this to get verbose output *)
  Log.write := (fun _ -> Lwt.return ())

let (>>|=) x f =
  x >>= function
  | `Ok y -> f y
  | `Error _ -> assert false

let id x = x

let make_queue () =
  Fake_block.connect () >>|=
  BC.connect >>|=
  Fat1.connect >>|= fun fs ->
  Fat1.format fs (Int64.of_int Fake_block.size) >>|= fun () ->
  Q.create fs

let read {Upload_queue.size; data} =
  Lwt_stream.to_list data >>= fun items ->
  let contents = String.concat "" items in
  assert_equal size (String.length contents |> Int64.of_int);
  return contents

let check_download q expected =
  Q.Download.peek q >>= read >>= fun contents ->
  assert_equal ~printer:id expected contents;
  Q.Download.delete q

let with_queue fn : unit =
  Lwt_unix.run (make_queue () >>= fn)

let suite = "queue" >:::[
  "mem_cache" >:: (fun () ->
    let mc = Memory_cache.create 3 in

    let check expected actual =
      let printer = function
        | None -> "None"
        | Some x -> x in
      assert_equal ~printer expected actual in

    Memory_cache.get mc 5L |> check None;
    Memory_cache.put mc 5L "five";
    Memory_cache.get mc 5L |> check (Some "five");

    Memory_cache.put mc 6L "six";
    Memory_cache.put mc 7L "seven";
    Memory_cache.put mc 8L "eight";
    Memory_cache.get mc 5L |> check None;
    Memory_cache.get mc 6L |> check (Some "six");
    Memory_cache.get mc 7L |> check (Some "seven");
    Memory_cache.get mc 8L |> check (Some "eight");

    Memory_cache.get mc 6L |> check (Some "six");
    Memory_cache.put mc 9L "nine";

    Memory_cache.get mc 6L |> check (Some "six");
    Memory_cache.get mc 7L |> check None;
    Memory_cache.get mc 8L |> check (Some "eight");
    Memory_cache.get mc 9L |> check (Some "nine");
  );

  "full" >:: (fun () -> with_queue (fun q ->
    let upload x =
      let data = string_of_int x in
      let item = { Upload_queue.
        size = String.length data |> Int64.of_int;
        data = Lwt_stream.of_list [data];
      } in
      Q.Upload.add q item in
    for_lwt x = 1 to 5 do upload x >>|= return done >>= fun () ->
    upload 6 >>= function
    | `Ok () | `Error (`Wrong_size _) -> assert false
    | `Error (`Unknown _) ->
    (* Note: we get Failure("Unknown error: Invalid_argument(\"invalid bounds (index 29184, length 4096)\")") *)

    for_lwt x = 1 to 5 do
      let expected = string_of_int x in
      check_download q expected
    done
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
    Fake_block.connect () >>|=
    BC.connect >>|=
    Fat1.connect >>|= fun fs ->
    Fat1.format fs (Int64.of_int Fake_block.size) >>|= fun () ->
    Q.create fs >>= fun q ->

    let stream1, push1 = Lwt_stream.create () in
    let stream2, push2 = Lwt_stream.create () in

    Q.Upload.add q { Upload_queue.size = 5L; data = stream1 } |> ignore;
    Q.Upload.add q { Upload_queue.size = 6L; data = stream2 } |> ignore;

    (* We start uploading stream1 first, but only stream2 finishes. *)
    push1 (Some "First");
    push2 (Some "Second");
    push2 None;

    (* Crash, reboot... *)
    Q.create fs >>= fun q ->

    Q.Upload.add q { Upload_queue.size = 5L; data = Lwt_stream.of_list ["hello"] } >>|= fun () ->

    check_download q "Second" >>= fun () ->
    check_download q "hello"
  end);

  ]

let () =
  Printexc.record_backtrace true;
  suite |> run_test_tt_main |> ignore;
  Format.print_newline ()
