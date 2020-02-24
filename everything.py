import csv
import os
import subprocess
from collections import defaultdict

import human_bytes_converter


def et_get_files_by_size(paths):
    " -date-created -date-modified "
    # 注意 sizedupe会找出所有文件(而不是指定的文件夹)中 大小重复的文件，
    # 然后再进行文件夹过滤，因此会多出部分文件大小只出现一次的文件
    args = ['-filename-column', '-size', '-sort', 'size', 'sizedupe:']
    args += ['"%s"' % i for i in paths]
    args_str = " ".join(args)

    files_by_size = defaultdict(list)
    for row in et_search(args_str):
        full_path = row["Filename"]
        file_size = row["Size"]
        files_by_size[file_size].append(full_path)
    return files_by_size


def et_path():
    stdout = subprocess.getoutput("powershell (Get-Process everything).Path").rstrip()
    lines = stdout.splitlines()
    if len(lines) == 1 and os.path.exists(lines[0]):
        return lines[0]
    raise RuntimeError("process everything.exe Not Found")
    # stdout = stdout


def et_search(args):
    temp_csv = "test.csv"
    command = "es -export-csv " + temp_csv + " " + args

    # TODO: error handling
    subprocess.getoutput(command)
    with open(temp_csv, "rt", encoding="utf-8") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            yield row


def export_dump_result_to_efu(result):
    # efu排序 (默认为size
    rows = []
    for hash_, files in result.items():
        size = os.path.getsize(files[0])
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
            f.write(line)

        et = et_path()
        subprocess.Popen([et, efu_file])
