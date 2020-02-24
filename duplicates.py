#!/usr/bin/env python
"""
Fast duplicate file finder.
Usage: duplicates.py <folder> [<folder>...]
Based on https://stackoverflow.com/a/36113168/300783
Based on https://gist.github.com/tfeldmann/fc875e6630d11f2256e746f67a09c1ae
Modified for Python3 with some small code improvements.
Add Everything backend support.
"""
import hashlib
import os
import sys
from collections import defaultdict

import everything


def chunk_reader(fobj, chunk_size=1024):
    """ Generator that reads a file in chunks of bytes """
    while True:
        chunk = fobj.read(chunk_size)
        if not chunk:
            return
        yield chunk


def get_hash(filename, first_chunk_only=False, hash_algo=hashlib.sha1):
    hashobj = hash_algo()
    with open(filename, "rb") as f:
        if first_chunk_only:
            hashobj.update(f.read(1024))
        else:
            for chunk in chunk_reader(f):
                hashobj.update(chunk)
    return hashobj.digest()


def get_files_by_size(paths):
    files_by_size = defaultdict(list)
    for path in paths:
        for dirpath, dirnames, filenames in os.walk(path):
            for filename in filenames:
                full_path = os.path.join(dirpath, filename)
                try:
                    # if the target is a symlink (soft one), this will
                    # dereference it - change the value to the actual target file
                    full_path = os.path.realpath(full_path)
                    file_size = os.path.getsize(full_path)
                except OSError:
                    # not accessible (permissions, etc) - pass on
                    continue
                files_by_size[file_size].append(full_path)
    return files_by_size


def check_for_duplicates(paths, filename_must_equal=False):
    # TODO: key为filename+size
    files_by_small_hash = defaultdict(list)
    files_by_full_hash = defaultdict(list)

    # files_by_size = get_files_by_size(paths)
    files_by_size = everything.et_get_files_by_size(paths)
    print(files_by_size)
    # exit()
    # For all files with the same file size, get their hash on the first 1024 bytes
    for files in files_by_size.values():
        if len(files) < 2:
            continue  # this file size is unique, no need to spend cpu cycles on it

        for filename in files:
            try:
                small_hash = get_hash(filename, first_chunk_only=True)
                files_by_small_hash[small_hash].append(filename)
            except OSError:
                # the file access might've changed till the exec point got here
                continue

    # For all files with the hash on the first 1024 bytes, get their hash on the full
    # file - collisions will be duplicates
    for files in files_by_small_hash.values():
        if len(files) < 2:
            # the hash of the first 1k bytes is unique -> skip this file
            continue

        for filename in files:
            try:
                full_hash = get_hash(filename, first_chunk_only=False)
                if full_hash in files_by_full_hash:
                    duplicate = files_by_full_hash[full_hash]
                    print("Duplicate found:\n - %s\n - %s\n" % (filename, duplicate))

                files_by_full_hash[full_hash].append(filename)
            except OSError:
                # the file access might've changed till the exec point got here
                continue

    dump_files = {hash_: files for hash_, files in files_by_full_hash.items() if len(files) > 1}
    # print(dump_files)
    everything.export_dump_result_to_efu(dump_files)


if __name__ == "__main__":
    if sys.argv[1:]:
        check_for_duplicates(sys.argv[1:])
    else:
        print("Usage: %s <folder> [<folder>...]" % sys.argv[0])
