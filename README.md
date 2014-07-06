0repo-queue
===========

Copyright Thomas Leonard, 2014


Introduction
------------

0repo-queue is a simple REST-based web-service designed to queue packages
submitted to 0repo. It can run as an ordinary Unix process or as a
self-contained "unikernel" to run as a Xen guest VM.


STATUS: work-in-progress, not ready for use!


Setup
-----

Install the `mirage` tool, as described here:

  http://openmirage.org/wiki/install

Edit `config.ml` to set the desired port (default: 8080).
Then configure either for Unix (`--unix`) or for Xen (`--xen`):

    $ mirage configure --unix

Install build dependencies:

    $ make depend

Build:

    $ make

To run as a Unix process, create a FAT16-formatted `disk.img` and run:

    $ ./mir-queue


Conditions
----------

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA 02111-1307  USA

The feed.xsl and feed.css stylesheets have their own license; see
the file headers for details.


Bug Reports
-----------

Please report any bugs to [the 0install mailing list](http://0install.net/support.html).
