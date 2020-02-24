# Duplicate File Scanner Based on Everything

- Ultra Fast (Based on Everything (NTFS USN, indexes)
- UI Based on Everything *.efu files.

## Usage

1. start everything
2. `python duplicates.py <folder> [<folder>...]`
3. manage files in everything window

## strategy

1. group by size OR size+filename (TODO)
2. timestamp
3. group by hash of first chunk

## Thanks

- [Everything](https://www.voidtools.com)
- [Everything CLI](https://www.voidtools.com/zh-cn/support/everything/command_line_interface/)
- <https://stackoverflow.com/a/36113168/300783>
- <https://gist.github.com/tfeldmann/fc875e6630d11f2256e746f67a09c1ae>
- human_bytes_converter.py By Giampaolo Rodola