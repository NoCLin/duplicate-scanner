#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Based on https://stackoverflow.com/a/36113168/300783
Based on https://gist.github.com/tfeldmann/fc875e6630d11f2256e746f67a09c1ae
Modified for Python3 with some small code improvements.
Add Everything backend support.
"""

import hashlib
import logging
import multiprocessing
import os
import queue
import sys
import threading
from collections import defaultdict

import xxhash

import everything
from utils import print_execute_time

logging.basicConfig(level=logging.INFO, filemode="wt", filename="log.log")


def get_hash(filename, hash_algo=hashlib.sha1, small_hash_mode=False):
    hashobj = hash_algo()
    chunk_size = 1024
    with open(filename, "rb") as f:
        if small_hash_mode:
            hashobj.update(f.read(chunk_size))
        else:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                hashobj.update(chunk)

    return hashobj.digest()


@print_execute_time()
def group_files_by_meta_walk(paths):
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


@print_execute_time()
def files_group_by_hash(files, hash_algo=hashlib.md5, small_hash_mode=False, thread_num=1):
    # For all files with the same file size, get their hash on the first 1024 bytes
    # simplify
    group_by = defaultdict(list)
    q = queue.Queue()
    for file in files:
        q.put(file)

    def checksum_thread(thread_id):
        while not q.empty():
            try:
                file_path = q.get(block=False)
                # 多线程
                _hash = get_hash(file_path, hash_algo=hash_algo, small_hash_mode=small_hash_mode)
                # print(thread_id, "checksum", file_path)
                group_by[_hash].append(file_path)
            except (PermissionError, FileNotFoundError) as e:
                pass

    thread_list = []

    for i in range(0, thread_num):
        t = threading.Thread(target=checksum_thread, args=(i,))
        thread_list.append(t)
        t.start()
        print("Thread %s started." % t.name)

    for t in thread_list:
        t.join()
        print("Thread %s completed." % t.name)
    print("done")
    return group_by



def get_files_from_groups(_dict):
    for files in _dict.values():
        for f in files:
            yield f


HASH_MODE_NONE = 0
HASH_MODE_SMALL = 1
HASH_MODE_FULL = 2


def check_for_duplicates(paths,
                         filename_must_equal=False,
                         modified_date_must_equal=False,
                         hash_mode=HASH_MODE_NONE,
                         max_size=None,
                         min_size=None,
                         endswith=None,
                         not_endswith=None
                         ):
    # 文件大小
    # 文件类型
    # size:<10240 size:>5000
    # endwith:.dll|.sys|.exe
    # !endwith:.dll !endwith:exe

    extra_search = ""

    def get_dupe_group(_dict):

        return {k: v for k, v in _dict.items() if len(v) > 1}

    files_group_by_meta = everything.files_group_by_meta(paths,
                                                         extra_search_text=extra_search,
                                                         flag_same_filename=filename_must_equal,
                                                         flag_same_modified_date=modified_date_must_equal)
    # files_group_by_meta = group_files_by_meta_walk(paths)
    files_group_by_meta_dupe = get_dupe_group(files_group_by_meta)
    print("files", len(list(get_files_from_groups(files_group_by_meta_dupe))))
    if hash_mode == HASH_MODE_NONE:
        # 不需要计算hash 直接返回
        return files_group_by_meta_dupe

    # TODO: emit 进度
    group_by_small_hash = files_group_by_hash(get_files_from_groups(files_group_by_meta_dupe),
                                              hash_algo=xxhash.xxh32,
                                              small_hash_mode=True,
                                              # thread_num=multiprocessing.cpu_count()
                                              )
    group_by_small_hash_dupe = get_dupe_group(group_by_small_hash)

    print("files", len(list(get_files_from_groups(group_by_small_hash_dupe))))

    if hash_mode == HASH_MODE_SMALL:
        # 不需要计算 full hash 直接返回
        return group_by_small_hash_dupe
    group_by_full_hash = files_group_by_hash(get_files_from_groups(group_by_small_hash_dupe),
                                             hash_algo=xxhash.xxh32,
                                             thread_num=multiprocessing.cpu_count(),
                                             small_hash_mode=False)
    group_by_full_hash_dupe = get_dupe_group(group_by_full_hash)
    print("files", len(list(get_files_from_groups(group_by_full_hash_dupe))))
    return group_by_full_hash_dupe


def main():
    if sys.argv[1:]:
        result = check_for_duplicates(sys.argv[1:],
                             hash_mode=HASH_MODE_SMALL,
                             filename_must_equal=True
                             )
        everything.export_dump_result_to_efu(result)
    else:
        print("Usage: %s <folder> [<folder>...]" % sys.argv[0])


if __name__ == "__main__":
    # import profile
    # profile.run('main()')
    main()
