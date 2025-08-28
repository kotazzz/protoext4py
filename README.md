
# PyExt4FS: A Python ext4-like Filesystem Simulator

<p align="center">
  <a href="README_RU.md">Русская версия</a> | English
</p>

![Python Version](https://img.shields.io/badge/python-3.12%2B-blue)
![License](https://img.shields.io/badge/license-MIT-green)

PyExt4FS is an educational project that simulates a modern, ext4-like filesystem from scratch in Python. It is designed to be a tool for understanding the internal workings of filesystems, including data structures like inodes, extent-based B+ trees, block groups, and bitmaps.

This is **not** a kernel module or a real filesystem driver. It operates on a single file (`fs.img`) that acts as a virtual block device. It is not compatible with actual ext4 partitions but faithfully implements many of its core concepts.

## Table of Contents

- [PyExt4FS: A Python ext4-like Filesystem Simulator](#pyext4fs-a-python-ext4-like-filesystem-simulator)
  - [Table of Contents](#table-of-contents)
  - [✨ Features](#-features)
  - [🏗️ Project Structure](#️-project-structure)
  - [🚀 Getting Started](#-getting-started)
    - [Prerequisites](#prerequisites)
    - [Installation \& Setup](#installation--setup)
  - [💻 Usage Example](#-usage-example)
  - [📚 Filesystem API (`fsapi.py`)](#-filesystem-api-fsapipy)
  - [💾 On-Disk Data Structures](#-on-disk-data-structures)
    - [Superblock](#superblock)
    - [Group Descriptor](#group-descriptor)
    - [Inode](#inode)
    - [Extent B+ Tree](#extent-b-tree)
    - [Directory Entry](#directory-entry)
  - [🔮 Future Work](#-future-work)
  - [📜 License](#-license)

## ✨ Features

- **ext4-like Design**: Implements core concepts from the Fourth Extended Filesystem.
- **Superblock**: Contains global metadata about the filesystem.
- **Block Groups**: Divides the virtual disk into manageable chunks, each with its own bitmaps and inode table.
- **Inodes**: Stores metadata for every file and directory.
- **Extent B+ Trees**: Efficiently tracks block allocation for files, avoiding the limitations of direct/indirect pointers for large files.
- **Bitmaps**: Tracks free inodes and data blocks within each group.
- **Hierarchical Directory Structure**: Standard file and directory organization.
- **File Operations**: Full support for creating, reading, writing, and deleting files.
- **Directory Operations**: Create, delete (recursively), and list directories.
- **Links**: Supports both hard links and symbolic links.
- **Interactive Shell**: A simple, UNIX-like command-line interface to interact with the filesystem.

## 🏗️ Project Structure

The project is organized into several key files:

- **`fs.py`**: Defines the core on-disk data structures (`Superblock`, `GroupDesc`, `Inode`, `ExtentHeader`, `ExtentLeaf`, `ExtentIndex`). Includes methods for packing these structures into bytes (`pack`) and unpacking them (`unpack`).

- **`fsapi.py`**: Implements the main `FileSystem` class, which provides a high-level API for all filesystem operations. This is the core logic engine, handling tasks like block/inode allocation, path resolution, and B+ tree manipulation.

- **`mkfs.py`**: A utility script to create and format a new filesystem image. It initializes the superblock, creates block groups, allocates bitmaps and inode tables, and sets up the root directory.

- **`shell.py`**: An interactive command-line shell that allows users to interact with the filesystem. It uses `fsapi.py` to execute commands like `ls`, `cd`, `mkdir`, `rm`, `cat`, `echo`, etc.

## 🚀 Getting Started

### Prerequisites

- Python 3.7 or newer
- `pip` for installing packages

### Installation & Setup

1. **Clone the repository:**

    ```sh
    git clone https://github.com/your-username/pyext4fs.git
    cd pyext4fs
    ```

2. **Install dependencies:**

    ```sh
    pip install -r requirements.txt
    ```

3. **Create the filesystem image:**
    This command will generate an 8MB `fs.img` file in the project directory.

    ```sh
    python mkfs.py
    ```

4. **Run the interactive shell:**
    Pass the image file path as an argument to the shell.

    ```sh
    python shell.py fs.img
    ```

    You should now see the shell prompt:

    ```plaintext
    / >
    ```

## 💻 Usage Example

Here is a sample session demonstrating basic commands in the `shell.py` interface.

```sh
# You are at the root directory
/ > ls
# (empty)

# Create a new directory
/ > mkdir documents

# List contents to see the new directory
/ > ls
documents

# Change into the new directory
/ > cd documents

# Create a file using echo redirection
/documents > echo "This is my first file." > report.txt

# List the contents of the 'documents' directory with details
/documents > lsd
Inode  Mode       Links  Uid  Gid    Size Name
------------------------------------------------------------
   12 -rw-r--r--   1    0    0     23B report.txt

# Display the file's content
/documents > cat report.txt
This is my first file.

# Create a larger, random file of 1MB
/documents > rndfile data.bin 1M
Created data.bin with 1048576 bytes of random ASCII data

# Check disk usage
/documents > df
Filesystem     1M-blocks  Used Available Use% Mounted on
rootfs                 8     1         7  17% /

# Exit the shell
/documents > exit
```

## 📚 Filesystem API (`fsapi.py`)

The `fsapi.py` module exposes a set of public functions for interacting with the filesystem programmatically.

| Function | Description |
|---|---|
| `init_filesystem(path)` | Initializes the global filesystem instance from an image file. Must be called first. |
| `get_filesystem()` | Returns the active `FileSystem` instance. |
| `openf(path, flags, mode)` | Opens a file and returns a file descriptor (int). `flags` can be `O_RDONLY`, `O_WRONLY`, `O_RDWR`, `O_CREAT`, `O_TRUNC`. |
| `read(fd, size, offset)` | Reads `size` bytes from the file descriptor `fd`. If `offset` is `None`, reads from the current position. |
| `write(fd, data, offset)` | Writes `data` (bytes) to the file descriptor `fd`. If `offset` is `None`, writes to the current position. |
| `close(fd)` | Closes an open file descriptor. |
| `unlink(path)` | Deletes a file (hard link). If the link count drops to zero, frees the inode and data blocks. |
| `mkdir(path, mode)` | Creates a new directory. |
| `rmdir(path)` | Removes an empty directory. |
| `rmdir_recursive(path)` | Removes a directory and all its contents. |

- `readdir(path)` | Returns a list of filenames in a directory. |
| `stat(path)` | Returns a dictionary of metadata for a file or directory, following symbolic links. |
| `lstat(path)` | Same as `stat`, but does not follow symbolic links. |

## 💾 On-Disk Data Structures

The filesystem's layout is inspired by ext4 and uses several key structures. All integers are stored in little-endian format.

### Superblock

Located at the beginning of the disk image (offset 0). Size: 56 bytes.

| Field | Size (bytes) | Description |
|---|---|---|
| `fs_size_blocks` | 8 | Total number of blocks in the filesystem. |
| `block_size` | 4 | Block size in bytes (e.g., 4096). |
| `blocks_per_group` | 4 | Number of blocks in a block group. |
| `inodes_per_group` | 4 | Number of inodes in a block group. |
| `total_inodes` | 8 | Total number of inodes. |
| `free_blocks_count`| 8 | Count of free blocks. |
| `free_inodes_count`| 8 | Count of free inodes. |
| `first_data_block` | 4 | First usable data block (usually 1). |
| `checksum` | 4 | CRC32 checksum of the superblock data. |

### Group Descriptor

An array of these descriptors is located right after the superblock. Size: 32 bytes per descriptor.

| Field | Size (bytes) | Description |
|---|---|---|
| `block_bitmap_block`| 8 | Block number of the block bitmap. |
| `inode_bitmap_block`| 8 | Block number of the inode bitmap. |
| `inode_table_block` | 8 | Starting block number of the inode table. |
| `free_blocks_count` | 4 | Number of free blocks in this group. |
| `free_inodes_count` | 4 | Number of free inodes in this group. |

### Inode

Contains metadata about a file or directory. Size: 88 bytes.

| Field | Size (bytes) | Description |
|---|---|---|
| `mode` | 4 | File type and permissions. |
| `uid`, `gid` | 4, 4 | User ID and Group ID. |
| `size_lo`, `size_high`| 4, 4 | 64-bit file size. |
| `links_count` | 4 | Number of hard links. |
| `atime`, `ctime`, `mtime`| 4, 4, 4 | Access, change, and modification timestamps. |
| `flags` | 4 | File flags. |
| `extent_root` | 48 | The root node of the extent B+ tree (inline). |

### Extent B+ Tree

The `extent_root` field in the inode contains a 12-byte header and 36 bytes for entries. If the tree grows, its nodes are stored in separate data blocks.

- **ExtentHeader (12 bytes)**: `magic`, `entries_count`, `max_entries`, `depth`.
- **ExtentIndex (12 bytes, in index nodes)**: `logical_block`, `child_block`. Points to the next level of the tree.
- **ExtentLeaf (12 bytes, in leaf nodes)**: `logical_block`, `block_count`, `start_block`. Points to a contiguous chunk of data blocks on disk.

### Directory Entry

Variable-sized records within a data block assigned to a directory.

| Field | Size (bytes) | Description |
|---|---|---|
| `inode_num` | 4 | Inode number of the entry. |
| `entry_len` | 4 | Total length of this entry record (rounded up). |
| `name_len` | 4 | Length of the filename. |
| `file_type` | 1 | File type (directory, regular file, etc.). |
| `reserved` | 1 | Reserved byte. |
| `name` | `name_len` | The filename (UTF-8 encoded). |

## 🔮 Future Work

This project provides a solid foundation that can be extended in many ways:

- **Journaling**: Implement a JBD2-like journal for crash consistency.
- **Permissions**: Add full support for user/group permissions and ownership checks.
- **Extended Attributes (xattrs)**: Store additional key-value metadata with inodes.
- **Caching**: Implement a block cache to improve performance.
- **FUSE Adapter**: Create a [Filesystem in Userspace (FUSE)](https://github.com/libfuse/libfuse) binding to mount the `fs.img` file as a real, browsable directory on Linux/macOS.
- **Performance Optimizations**: Refactor critical code paths for better speed and efficiency.

## 📜 License

This project is licensed under the MIT License. See the `LICENSE` file for details.
