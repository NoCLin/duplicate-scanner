# -*- coding: utf-8 -*-
import ctypes
import datetime
import os
import struct
import subprocess
# defines
from collections import defaultdict
from ctypes.wintypes import LPCWSTR

from utils import human_bytes_converter, print_execute_time

EVERYTHING_REQUEST_FILE_NAME = 0x00000001
EVERYTHING_REQUEST_PATH = 0x00000002
EVERYTHING_REQUEST_FULL_PATH_AND_FILE_NAME = 0x00000004
EVERYTHING_REQUEST_EXTENSION = 0x00000008
EVERYTHING_REQUEST_SIZE = 0x00000010
EVERYTHING_REQUEST_DATE_CREATED = 0x00000020
EVERYTHING_REQUEST_DATE_MODIFIED = 0x00000040
EVERYTHING_REQUEST_DATE_ACCESSED = 0x00000080
EVERYTHING_REQUEST_ATTRIBUTES = 0x00000100
EVERYTHING_REQUEST_FILE_LIST_FILE_NAME = 0x00000200
EVERYTHING_REQUEST_RUN_COUNT = 0x00000400
EVERYTHING_REQUEST_DATE_RUN = 0x00000800
EVERYTHING_REQUEST_DATE_RECENTLY_CHANGED = 0x00001000
EVERYTHING_REQUEST_HIGHLIGHTED_FILE_NAME = 0x00002000
EVERYTHING_REQUEST_HIGHLIGHTED_PATH = 0x00004000
EVERYTHING_REQUEST_HIGHLIGHTED_FULL_PATH_AND_FILE_NAME = 0x00008000

# dll imports
dll = ctypes.WinDLL("Everything64.dll")
dll.Everything_GetResultDateModified.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_ulonglong)]
dll.Everything_GetResultDateCreated.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_ulonglong)]
dll.Everything_GetResultSize.argtypes = [ctypes.c_int, ctypes.POINTER(ctypes.c_ulonglong)]
dll.Everything_GetResultFileNameW.argtypes = [ctypes.c_int]
dll.Everything_GetResultFileNameW.restype = LPCWSTR

# convert a windows FILETIME to a python datetime
# https://stackoverflow.com/questions/39481221/convert-datetime-back-to-windows-64-bit-filetime
WINDOWS_TICKS = int(1 / 10 ** -7)  # 10,000,000 (100 nanoseconds or .1 microseconds)
WINDOWS_EPOCH = datetime.datetime.strptime('1601-01-01 00:00:00',
                                           '%Y-%m-%d %H:%M:%S')
POSIX_EPOCH = datetime.datetime.strptime('1970-01-01 00:00:00',
                                         '%Y-%m-%d %H:%M:%S')
EPOCH_DIFF = (POSIX_EPOCH - WINDOWS_EPOCH).total_seconds()  # 11644473600.0
WINDOWS_TICKS_TO_POSIX_EPOCH = EPOCH_DIFF * WINDOWS_TICKS  # 116444736000000000.0


def filetime_to_datetime(filetime):
    """Convert windows filetime winticks to python datetime.datetime."""
    winticks = struct.unpack('<Q', filetime)[0]
    microsecs = (winticks - WINDOWS_TICKS_TO_POSIX_EPOCH) / WINDOWS_TICKS
    return datetime.datetime.fromtimestamp(microsecs)


@print_execute_time()
def files_group_by_meta(paths, extra_search_text="", flag_same_filename=False, flag_same_modified_date=False):
    search_text = "sizedupe: " + "|".join(paths) + " " + extra_search_text
    print(search_text)
    # setup search
    dll.Everything_SetSearchW(search_text)

    request_flags = EVERYTHING_REQUEST_FILE_NAME | EVERYTHING_REQUEST_PATH | EVERYTHING_REQUEST_SIZE
    if flag_same_modified_date:
        request_flags |= EVERYTHING_REQUEST_DATE_MODIFIED
    # EVERYTHING_REQUEST_DATE_CREATED
    dll.Everything_SetRequestFlags(request_flags)

    # execute the query
    dll.Everything_QueryW(1)

    # get the number of results
    num_results = dll.Everything_GetNumResults()

    # show the number of results
    print("Result Count: {}".format(num_results))

    # create buffers
    full_path = ctypes.create_unicode_buffer(260)
    date_modified_filetime = ctypes.c_ulonglong(1)
    date_created_filetime = ctypes.c_ulonglong(1)
    file_size = ctypes.c_ulonglong(1)

    group_by = defaultdict(list)

    # show results
    for i in range(num_results):
        dll.Everything_GetResultFullPathNameW(i, full_path, 260)
        dll.Everything_GetResultSize(i, file_size)
        file_name = dll.Everything_GetResultFileNameW(i)
        if flag_same_modified_date:
            dll.Everything_GetResultDateModified(i, date_modified_filetime)
        key = "%d.%s.%s" % (
            file_size.value,
            file_name if flag_same_filename else "",
            struct.unpack('<Q', date_modified_filetime)[0] if flag_same_modified_date else ""
        )
        group_by[key].append(ctypes.wstring_at(full_path))

    # NOTE:
    # b'C:\\ProgramData\\Microsoft\\Provisioning\\{c8a326e4-f518-4f14-b543-97a57e1a975e}\\Prov\\RunTime\\242__Connections_Cellular_Bit\xc4\x97 Lietuva (Lithuania)_i0$(__MVID)@WAP.provxml'
    # UnicodeEncodeError: 'gbk' codec can't encode character '\u0117' in position 726: illegal multibyte sequence

    return group_by


def get_running_everything_path():
    stdout = subprocess.getoutput("powershell (Get-Process everything).Path").rstrip()
    lines = stdout.splitlines()
    if len(lines) == 1 and os.path.exists(lines[0]):
        return lines[0]
    raise RuntimeError("process everything.exe Not Found")
    # stdout = stdout


def export_dump_result_to_efu(result):
    # efu排序 (默认为size
    rows = []
    for hash_, files in result.items():
        try:
            size = os.path.getsize(files[0])
        except:
            continue
        for file in files:
            # TODO: real data
            row = {'Filename': file, 'Size': size, 'Date Modified': "",
                   'Date Created': "", 'Attributes': 0}
            rows.append(row)

        fake_name = r"A:\\" + " " * 20 + \
                    " Files:%d" % (len(files)) + \
                    " Extra Size: " + human_bytes_converter.bytes2human(size * (len(files) - 1))

        rows.append({'Filename': fake_name, 'Size': size, 'Date Modified': "",
                     'Date Created': "", 'Attributes': 0})

    fieldnames = ['Filename', 'Size', 'Date Modified', 'Date Created', 'Attributes']
    efu_file = "result.efu"

    with open(efu_file, "wt", encoding="utf-8") as f:
        f.write(",".join(fieldnames) + "\n")
        for row in rows:
            line = "%s\n" % (",".join([str(row[field]) for field in fieldnames]))
            try:
                f.write(line)
            except:
                pass

        et = get_running_everything_path()
        subprocess.Popen([et, efu_file])


if __name__ == '__main__':
    files_group_by_meta(["C:"], "", True, True)
