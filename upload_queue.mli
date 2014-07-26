(* Copyright (C) 2014, Thomas Leonard
 * See the README file for details. *)

(** An upload.
 * To avoid loading complete uploads into RAM, we stream them between
 * the network and the disk. *)
type item = {
  size : int64;
  data : string Lwt_stream.t;
}

type add_error = [`Wrong_size of int64 | `Disk_full | `Unknown of exn]

module Make : functor (B : V1_LWT.BLOCK) ->
  sig
    (** An upload queue. *)
    type t

    (** Create a new queue, backed by a filesystem. *)
    val create : B.t -> t Lwt.t

    module Upload : sig
      (** Add an upload to the queue.
       * The upload is added only once the end of the stream is reached, and
       * only if the total size matches the size in the record.
       * To cancel an add, just terminate the stream. *)
      val add : t -> item -> [ `Ok of unit | `Error of add_error ] Lwt.t
    end

    module Download : sig
      (** Interface for the repository software to fetch items from the queue.
       * Only one client may use this interface at a time, or things will go
       * wrong. *)

      (** Return a fresh stream for the item at the head of the queue, without
       * removing it. After downloading it sucessfully, the client should call
       * [delete]. If the queue is empty, this blocks until an item is
       * available. *)
      val peek : t -> item Lwt.t

      (** Delete the item previously retrieved by [peek].
       * If the previous item has already been deleted, this does nothing,
       * even if there are more items in the queue. *)
      val delete : t -> unit Lwt.t
    end
  end
