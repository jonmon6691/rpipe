#!/usr/bin/env python
#
# MIT License
#
# Copyright (c) 2017 Eric A. Borisch
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

from __future__ import print_function

import argparse
import subprocess
import sys
import string

from hashlib import md5
from os import path,fsync,unlink,devnull
from tempfile import mkdtemp
from shutil import rmtree
from glob import glob
from StringIO import StringIO

epi="""
Works by creating temporary files of size --chunksize in --tempdir, and
uploading those. By default runs two 'jobs', such that an upload can be
occurring while the next chunk is being built. As such, tempdir needs to
be able to hold two chunks. They are deleted and checksum-ed along the
way, and verified during retrieval.

Make sure that your destination doesn't exist (purge it first.) This will
likely be added as a default step on a future version.

Examples:
    <some source> | rpipe.py remote:some/empty/loc
    <some source> | rpipe.py --nocheck crypt:an/encrypted/loc
                    ^ As we can't check the md5s of the deposited files on an
                      encrypted store...
    rpipe.py --replay remote:some/empty/loc | <some sink>
    rpipe.py --replay --nocheck crypt:an/encrypted/loc | <some sink>
"""

parser = argparse.ArgumentParser(description=
        'Provides pipe in to / out of rclone destination',
        epilog=epi, formatter_class=argparse.RawDescriptionHelpFormatter)
aa = parser.add_argument
aa('destination')

aa('-c', '--chunksize',
    type=int, default=2**23,
    help='Chunk size for splitting transfer [8MB]')

aa('-b', '--blocksize',
    type=int, default=2**16,
    help='Block size for read/write [64KB]')

aa('-t', '--tempdir',
    nargs=1, default=None,
    help='Directory for storing temporary files')

aa('-r', '--replay',
    action='store_true',
    help='Write previous saved stream to stdout')

aa('-j', '--jobs',
    type=int, default=2,
    help='Number of simultaneous rclone jobs')

aa('-n', '--nocheck',
    action='store_true',
    help='Don\'t check md5 at end (eg. crypto store)')

aa('--verify',
    action='store_true',
    help='Only check the integrity of the given remote by verifying checksums. Doesn\'t work with --nocheck. Returns 0 if checksums match, 1 if there is a problem')

aa('--parchive',
    action='store_true',
    help='Create and upload PAR2 files (parity archives) alongside the chunks. The parity file can repair damage up to 5%% of the total size of the file. Excellent protection against bitrot.')

aa('--repair',
    action='store_true',
    help='Whenever checksums don\'t match, look for a parity file and try and fix the data on the remote')


def mkname(n, width=6, prefix=''):
    """ Converts n into base-26 [a-z] """
    C = string.ascii_lowercase
    s = [C[0]] * width
    p = width - 1
    n = int(n)

    while n:
        if p < 0:
            raise(Exception, 'n too large for width!')
        s[p] = C[n % 26]
        n /= 26
        p -= 1

    return prefix + ''.join(s)


def readin(f, blk, tot, csums):
    """ reads up to "tot" bytes from stdin into "f" in "blk" size chunks,
        returns number of bytes read """
    fout = open(f, 'w', blk)
    maxlen = tot
    while tot:
        d = sys.stdin.read(min(blk,tot))
        if len(d):
            for c in csums:
                c.update(d)
            fout.write(d)
            tot -= len(d)
        else:
            break
    fout.flush()
    fsync(fout.fileno())
    fout.close()
    return maxlen - tot


def upload(f, dst):
    sp = subprocess.Popen(('rclone',
                           'copyto',
                           '--retries=10',
                           f,
                           path.join(dst,path.basename(f))))
    return sp


def cat(remote, fd=sys.stdout, bs=65536, csums=[], async=False):
    """ Streams a file from remote to 'fd' """
    sp = subprocess.Popen(('rclone',
                           'cat',
                           '--retries=10',
                           remote),
                           stdout = subprocess.PIPE)
    if async:
        # Only set up the process; return the Popen object
        return sp

    buf = (1,)
    tot = 0
    while len(buf) > 0:
        buf = sp.stdout.read(bs)
        if len(buf):
            tot += len(buf)
            fd.write(buf)
            for c in csums:
                c.update(buf)
    sp.wait()
    return len


def complete(flist, m):
    """ blocks on the rclone process to complete for the m'th chunk """
    if flist[m][2]:
        flist[m][2].wait()
        flist[m][2] = None
        unlink(flist[m][0])


class ChecksumError(Exception):
    pass


def check_pipe(remote):
    """ Check the files on the remote
    Compares actual checksums of chunk files on remote to checksums
    for them stored in rpipe.md5 (also on remote) when the chunk was
    originally sent.

    If the --repair flag is set, then this function will also try
    to repair and files with bad checksums if it can find a par2
    file on the remote.

    Returns an open buffer to the rpipe.md5 file on success, raises
    an exception if integrity isn't verified
     """
    remote_sums = {} # Dict that maps filenames to checksums

    rmd5 = subprocess.check_output(('rclone',
                                    'md5sum',
                                    '--include=rp-*',
                                    '--retries=10',
                                    remote))
    rmd5 = rmd5.split('\n')
    for l in rmd5:
        d = l.split() # (checksum, filename)
        if len(d) < 2:
            continue
        remote_sums[d[1]] = d[0]

    buf = StringIO()
    cat(path.join(remote, 'rpipe.md5'), fd=buf)
    buf.seek(0)
    md = {}

    for chunk_num, l in enumerate(buf): # Once per row in the rpipe.md5 file
        d = l.split() # Tuples (checksum, filename)

        if len(d) < 2 or d[1] == 'TOTAL':
            continue

        if d[1] not in remote_sums:
            print("Chunk missing: {}/{}".format(remote, d[1]))
            raise(ChecksumError, "Chunk missing {}/{}".format(remote, d[1]))

        if remote_sums[d[1]] != d[0]:
            chunk_id = d[1].split('-')[1]

            # Get a list of files from the remote, check for corresponding par-xxxxxx file
            files = subprocess.check_output(('rclone', 'lsf', remote)).split('\n')
            par_name = "par-{}.par2".format(chunk_id)
            if par_name in files:
                if args.repair:
                    dev_null = open(devnull, "w")
                    print("Repairing chunk {}... ".format(chunk_num), file=sys.stderr, end='')
                    
                    # download par file
                    subprocess.call(("rclone", "copy", path.join(remote, par_name), args.tempdir), stdout=dev_null)
                    
                    # download rp- file
                    rp_remote = path.join(remote, d[1])
                    subprocess.call(("rclone", "copy", rp_remote, args.tempdir), stdout=dev_null)

                    # Repair
                    par_tmp = path.join(args.tempdir, par_name)
                    rp_tmp = path.join(args.tempdir, d[1])
                    ret = subprocess.call(("par2", "repair", "-q", "-a", par_tmp, rp_tmp), stdout=dev_null)

                    if ret == 0: # Repair returns 0 on success
                        # Copy the repaired chunk back onto the remote
                        subprocess.call(("rclone", "copy", rp_tmp, remote), stdout=dev_null)
                        print("Done.", file=sys.stderr)
                    else:
                        raise ChecksumError("Couldn't repair remote data, use --nocheck to allow the data to be piped anyway.")

                    # Remove temporary files
                    dev_null.close()
                    unlink(par_tmp)
                    unlink(rp_tmp)
                    unlink("{}.1".format(rp_tmp))
                else: # --repair flag was not set
                    raise ChecksumError("ERROR: Checksum failed\nHOPE: parity file found, run again with --repair to try and repair".format(d[1]))

            else: # no par file found
                raise ChecksumError('Checksums do not match, use --nocheck to allow the data to be piped anyway')
    return buf


def deposit(args):
    """ Handle the whole transfer for sending """
    # Only output on stderr
    sys.stdout = sys.stderr
    subprocess.check_call(('rclone', 'mkdir', args.destination))
    n = 0
    flist = [] # List of tuples: (chunk-filename, md5 object, subprocess object)
    b = 1 # Bytes read from stdin
    totsum = md5() # Checksum for the whole transfer
    totsize = 0 # Accumulate bytes received on stdin
    while b > 0: # Executes once per arg.chunksize of input on stdin
        chunk_id = mkname(n)
        flist.append([path.join(args.tempdir, "rp-{}".format(chunk_id)), None, None])
        csum = md5() # Checksum for this chunk
        b = readin(flist[-1][0],
                   args.blocksize,
                   args.chunksize,
                   (csum, totsum))
        if n >= args.jobs:
            complete(flist, n - args.jobs)

        if b: # The chunk is non-zero in size
            totsize += b
            flist[n][1] = csum.hexdigest() # REPLACE md5 object with the final chunk digest

            if args.parchive:
                # Create the parchive
                subprocess.call(("par2", "create", "-q", "-q", "-n1", flist[n][0]))

                # The block file can also function as the index, so we'll delete the index
                unlink("{}.par2".format(flist[n][0]))

                # Rename the parchive block file, (.par2 extension is REQUIRED by par2 repair)
                parchive_name = path.join(args.tempdir, "par-{}.par2".format(chunk_id))
                par_file = glob("{}.vol*.par2".format(flist[n][0]))
                assert len(par_file) == 1, "There can be only one!"
                subprocess.call(("mv", par_file[0], parchive_name))

                # upload it
                upload(parchive_name, args.destination).wait()

                # Delete the temporary copy
                unlink(parchive_name)

            flist[n][2] = upload(flist[-1][0], args.destination) # Start the upload (returns a subprocess.Popen object)
            print('Sending chunk {} [{} bytes so far]'.format(n, totsize))
            if n == 0:
                # Wait for first (for directory creation)
                complete(flist, n)
            n+=1
        else:
            unlink(flist[-1][0])
    for x in range(len(flist)):
        complete(flist, x)

    print('Sending complete. Depositing metadata.')
    mdpath=path.join(args.tempdir, 'rpipe.md5')
    md = open(mdpath, 'w')
    for x in flist:
        if x[1]:
            print("{}  {}".format(x[1], path.basename(x[0])), file=md)
    print("{}  {}".format(totsum.hexdigest(), 'TOTAL'), file=md)
    md.flush()
    fsync(md.fileno())
    md.close()
    final = upload(mdpath, args.destination)
    final.wait()
    unlink(mdpath)
    if not args.nocheck:
        print('Final checksum checks.')
        check_pipe(args.destination)
        print('Success. Checksums match.')
    else:
        print('Complete. Skipped checksum match.')
    print('Wrote {} bytes into {}'.format(totsize, args.destination))
    print('Full stream checksum: {}'.format(totsum.hexdigest()))


def replay(args):
    if not args.nocheck:
        print("Checking initial integrity.", file=sys.stderr)
        buf = check_pipe(args.destination)
        print('Success. Checksums match.', file=sys.stderr)
    else:
        buf = StringIO()
        # We still open the rpipe.md5 file to check the total checksum
        cat(path.join(args.destination, 'rpipe.md5'), fd=buf)
    buf.seek(0)
    md = {}
    # Create mapping of filename to checksum, also used to know the full list of chunks
    for l in buf:
        d = l.split()
        if not len(d):
            continue
        md[d[1]] = d[0]

    n = 1 # Enumerate chunks for the status message
    tsum = md5() # Checksum for the total transfer
    tsize = 0 # Accumulate total size
    for f in sorted(md.keys()):
        if f == 'TOTAL':
            continue
        print("Retrieving {}/{} [{} bytes total]".format(n, len(md)-1, tsize),
              file=sys.stderr)
        p = cat(path.join(args.destination, f), async=True)
        csum = md5() # Checksum for this chunk
        buf = (1,)
        while len(buf):
            buf = p.stdout.read(args.blocksize)
            if not len(buf):
                continue # Will exit the while loop, could be a break... but honestly  do the following lines suffer from a zero-length buf?

            tsize += len(buf)
            csum.update(buf)
            tsum.update(buf)
            sys.stdout.write(buf)
        p.wait()
        n += 1

        if csum.hexdigest() != md[f]:
            print('WARNING: Checksum mis-match!!', file=sys.stderr)
            print('{} {} {}'.format(f,md[f],csum.hexdigest()), file=sys.stderr)
    print("Retrieved {} bytes total".format(tsize),
          file=sys.stderr)
    if tsum.hexdigest() != md['TOTAL']:
        print('WARNING: Full stream checksum mis-match!!', file=sys.stderr)


if __name__ == "__main__":
    args = parser.parse_args()
    ret = 0

    rmtemp = False
    if args.tempdir is None:
        args.tempdir = mkdtemp(prefix='rpipe-')
        rmtemp = True

    try:
        if args.verify:
            check_pipe(args.destination)
        elif args.replay:
            replay(args)
        else:
            deposit(args)
    except ChecksumError as e:
        print(e, file=sys.stderr)
        ret = 1
    finally:
        if rmtemp:
            rmtree(args.tempdir)
        exit(ret)

