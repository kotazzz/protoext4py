import os
import struct
import time
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from fs import Extent, Superblock, GroupDesc, Inode

# File system constants
BLOCK_SIZE = 4096
INODE_SIZE = (
    96  # Updated to match actual Inode structure size (rounded up to multiple of 4)
)
BLOCKS_PER_GROUP = 8192
INODES_PER_GROUP = 2048
ROOT_INODE = 2

# File types
S_IFMT = 0o170000  # File type mask
S_IFREG = 0o100000  # Regular file
S_IFDIR = 0o040000  # Directory

# File flags
O_RDONLY = 0o0  # Read only
O_WRONLY = 0o1  # Write only
O_RDWR = 0o2  # Read/write
O_CREAT = 0o100  # Create if not exists
O_TRUNC = 0o1000  # Truncate to zero length


@dataclass
class FileDescriptor:
    """File descriptor for open files"""

    inode_num: int
    path: str
    flags: int
    offset: int = 0
    inode: Optional[Inode] = None


@dataclass
class DirEntry:
    """Directory entry structure"""

    inode_num: int
    name_len: int
    name: str
    file_type: int = 0

    def pack(self) -> bytes:
        """Pack directory entry to bytes"""
        name_bytes = self.name.encode("utf-8")
        entry_len = 14 + len(
            name_bytes
        )  # 12 bytes header + 1 file_type + 1 reserved + name
        # Align to 4 bytes
        entry_len = ((entry_len + 3) // 4) * 4

        data = struct.pack("<III", self.inode_num, entry_len, self.name_len)
        data += bytes([self.file_type])
        data += b"\x00"  # reserved
        data += name_bytes
        data += b"\x00" * (entry_len - 14 - len(name_bytes))
        return data

    @classmethod
    def unpack(cls, data: bytes, offset: int = 0) -> Tuple["DirEntry", int]:
        """Unpack directory entry from bytes"""
        if len(data) < offset + 12:  # Need at least 12 bytes for header
            raise ValueError("Not enough data for directory entry")

        inode_num, entry_len, name_len = struct.unpack(
            "<III", data[offset : offset + 12]
        )

        # Handle empty/end of directory entries
        if inode_num == 0 or entry_len == 0:
            return None, 0

        if len(data) < offset + 12:
            raise ValueError("Not enough data for directory entry header")

        file_type = data[offset + 12] if len(data) > offset + 12 else 0

        if len(data) < offset + entry_len:
            raise ValueError("Directory entry length exceeds data")

        name_start = offset + 14  # 12 bytes header + 1 byte file_type + 1 byte reserved
        name = data[name_start : name_start + name_len].decode("utf-8", errors="ignore")

        return cls(inode_num, name_len, name, file_type), entry_len


class FileSystem:
    """Ext4-like filesystem implementation"""

    def __init__(self, image_path: str):
        self.image_path = image_path
        self.image_file = None
        self.superblock = None
        self.group_descriptors = []
        self.open_files: Dict[int, FileDescriptor] = {}
        self.next_fd = 3  # Start from 3 (after stdin, stdout, stderr)

        self._load_filesystem()

    def _load_filesystem(self):
        """Load filesystem metadata"""
        if not os.path.exists(self.image_path):
            raise FileNotFoundError(f"Filesystem image {self.image_path} not found")

        self.image_file = open(self.image_path, "r+b")

        # Load superblock
        self.image_file.seek(0)
        sb_data = self.image_file.read(56)  # Superblock size
        self.superblock = Superblock.unpack(sb_data)

        # Load group descriptors
        self.image_file.seek(BLOCK_SIZE)
        num_groups = (
            self.superblock.fs_size_blocks + BLOCKS_PER_GROUP - 1
        ) // BLOCKS_PER_GROUP

        for i in range(num_groups):
            current_offset = BLOCK_SIZE + i * 32
            self.image_file.seek(current_offset)
            gd_data = self.image_file.read(32)

            if len(gd_data) == 32:
                self.group_descriptors.append(GroupDesc.unpack(gd_data))

    def _get_inode(self, inode_num: int) -> Inode:
        """Get inode by number"""
        if inode_num == 0:
            raise ValueError("Invalid inode number 0")

        # Calculate which group contains this inode
        group_num = (inode_num - 1) // INODES_PER_GROUP
        inode_index = (inode_num - 1) % INODES_PER_GROUP

        if group_num >= len(self.group_descriptors):
            raise ValueError(f"Inode {inode_num} is beyond filesystem bounds")

        group_desc = self.group_descriptors[group_num]

        # Calculate inode offset
        inode_offset = (
            group_desc.inode_table_block * BLOCK_SIZE + inode_index * INODE_SIZE
        )

        self.image_file.seek(inode_offset)
        # Read only the actual size needed for Inode structure
        import struct

        actual_inode_size = struct.calcsize(Inode._fmt)
        inode_data = self.image_file.read(actual_inode_size)

        if len(inode_data) != actual_inode_size:
            raise ValueError(f"Could not read inode {inode_num}")

        return Inode.unpack(inode_data)

    def _write_inode(self, inode_num: int, inode: Inode):
        """Write inode to disk"""
        if inode_num == 0:
            raise ValueError("Invalid inode number 0")

        # Calculate which group contains this inode
        group_num = (inode_num - 1) // INODES_PER_GROUP
        inode_index = (inode_num - 1) % INODES_PER_GROUP

        if group_num >= len(self.group_descriptors):
            raise ValueError(f"Inode {inode_num} is beyond filesystem bounds")

        group_desc = self.group_descriptors[group_num]

        # Calculate inode offset
        inode_offset = (
            group_desc.inode_table_block * BLOCK_SIZE + inode_index * INODE_SIZE
        )

        self.image_file.seek(inode_offset)
        self.image_file.write(inode.pack())
        self.image_file.flush()

    def _write_superblock(self):
        self.image_file.seek(0)
        self.image_file.write(self.superblock.pack())
        self.image_file.flush()

    def _write_group_descriptor(self, group_num: int, group_desc: GroupDesc):
        self.image_file.seek(BLOCK_SIZE + group_num * 32)
        data = group_desc.pack()
        self.image_file.write(data)
        self.image_file.flush()

    def _allocate_inode(self) -> int:
        """Allocate a new inode"""
        for group_num, group_desc in enumerate(self.group_descriptors):
            if group_desc.free_inodes_count > 0:
                # Read inode bitmap
                self.image_file.seek(group_desc.inode_bitmap_block * BLOCK_SIZE)
                bitmap = bytearray(self.image_file.read(BLOCK_SIZE))

                # Find free inode
                for byte_idx in range(len(bitmap)):
                    if bitmap[byte_idx] != 0xFF:  # Not all bits set
                        for bit_idx in range(8):
                            if not (bitmap[byte_idx] & (1 << bit_idx)):
                                # Found free inode
                                bitmap[byte_idx] |= 1 << bit_idx

                                # Write bitmap back
                                self.image_file.seek(
                                    group_desc.inode_bitmap_block * BLOCK_SIZE
                                )
                                self.image_file.write(bitmap)

                                # Update group descriptor
                                group_desc.free_inodes_count -= 1
                                self.group_descriptors[group_num] = (
                                    group_desc  # Update in-memory copy
                                )
                                self._write_group_descriptor(group_num, group_desc)

                                # Update superblock
                                self.superblock.free_inodes_count -= 1
                                self._write_superblock()

                                return (
                                    group_num * INODES_PER_GROUP
                                    + byte_idx * 8
                                    + bit_idx
                                    + 1
                                )

        raise OSError("No free inodes available")

    def _allocate_block(self) -> int:
        """Allocate a new block"""
        for group_num, group_desc in enumerate(self.group_descriptors):
            if group_desc.free_blocks_count > 0:
                # Read block bitmap
                self.image_file.seek(group_desc.block_bitmap_block * BLOCK_SIZE)
                bitmap = bytearray(self.image_file.read(BLOCK_SIZE))

                # Find free block
                for byte_idx in range(len(bitmap)):
                    if bitmap[byte_idx] != 0xFF:  # Not all bits set
                        for bit_idx in range(8):
                            if not (bitmap[byte_idx] & (1 << bit_idx)):
                                # Found free block
                                bitmap[byte_idx] |= 1 << bit_idx

                                # Write bitmap back
                                self.image_file.seek(
                                    group_desc.block_bitmap_block * BLOCK_SIZE
                                )
                                self.image_file.write(bitmap)

                                # Update group descriptor
                                group_desc.free_blocks_count -= 1
                                self.group_descriptors[group_num] = (
                                    group_desc  # Update in-memory copy
                                )
                                self._write_group_descriptor(group_num, group_desc)

                                # Update superblock
                                self.superblock.free_blocks_count -= 1
                                self._write_superblock()

                                # Calculate actual block number, accounting for reserved blocks
                                allocated_block = (
                                    group_num * BLOCKS_PER_GROUP
                                    + byte_idx * 8
                                    + bit_idx
                                )

                                # For group 0, blocks 0-1 are reserved (superblock + group descriptors)
                                # Make sure we don't allocate reserved blocks
                                if group_num == 0 and allocated_block < 2:
                                    # Skip this block and continue searching
                                    continue

                                return allocated_block

        raise OSError("No free blocks available")

    def _free_block(self, block_num: int):
        """Free a block"""
        group_num = block_num // BLOCKS_PER_GROUP
        block_idx = block_num % BLOCKS_PER_GROUP

        if group_num >= len(self.group_descriptors):
            return

        group_desc = self.group_descriptors[group_num]

        # Read block bitmap
        self.image_file.seek(group_desc.block_bitmap_block * BLOCK_SIZE)
        bitmap = bytearray(self.image_file.read(BLOCK_SIZE))

        # Clear block bit
        byte_idx = block_idx // 8
        bit_idx = block_idx % 8
        bitmap[byte_idx] &= ~(1 << bit_idx)

        # Write bitmap back
        self.image_file.seek(group_desc.block_bitmap_block * BLOCK_SIZE)
        self.image_file.write(bitmap)

        # Update group descriptor
        group_desc.free_blocks_count += 1
        self.group_descriptors[group_num] = group_desc  # Update in-memory copy
        self._write_group_descriptor(group_num, group_desc)

        # Update superblock
        self.superblock.free_blocks_count += 1
        self._write_superblock()

    def _find_file_in_directory(self, dir_inode: Inode, filename: str) -> Optional[int]:
        """Find file in directory, return inode number"""
        if not (dir_inode.mode & S_IFDIR):
            return None

        # Read directory blocks
        for extent in dir_inode.extents:
            if extent.block_count == 0:
                break

            for block_offset in range(extent.block_count):
                block_num = extent.start_block + block_offset
                self.image_file.seek(block_num * BLOCK_SIZE)
                block_data = self.image_file.read(BLOCK_SIZE)

                # Parse directory entries
                offset = 0
                while offset < len(block_data):
                    try:
                        result = DirEntry.unpack(block_data, offset)
                        if result[0] is None:  # Empty entry or end of directory
                            break
                        entry, entry_len = result
                        if entry.name == filename:
                            return entry.inode_num
                        offset += entry_len

                        if entry_len == 0:  # Prevent infinite loop
                            break
                    except (ValueError, UnicodeDecodeError):
                        break

        return None

    def _add_directory_entry(
        self, dir_inode_num: int, filename: str, file_inode_num: int, file_type: int = 0
    ):
        """Add entry to directory"""
        dir_inode = self._get_inode(dir_inode_num)

        if not (dir_inode.mode & S_IFDIR):
            raise OSError("Not a directory")

        # Create new directory entry
        new_entry = DirEntry(file_inode_num, len(filename), filename, file_type)
        entry_data = new_entry.pack()

        # Find the current end of directory data
        current_size = 0
        for extent in dir_inode.extents:
            if extent.block_count == 0:
                break

            # For each block in this extent, scan to find actual used space
            for block_idx in range(extent.block_count):
                block_num = extent.start_block + block_idx
                self.image_file.seek(block_num * BLOCK_SIZE)
                block_data = self.image_file.read(BLOCK_SIZE)

                # Parse entries to find actual end
                offset = 0
                block_used = 0
                while offset < len(block_data):
                    try:
                        result = DirEntry.unpack(block_data, offset)
                        if result[0] is None:  # End of entries
                            break
                        entry, entry_len = result
                        if entry_len == 0:
                            break
                        offset += entry_len
                        block_used = offset
                    except (ValueError, UnicodeDecodeError):
                        break

                # If this is the last block and has space, append here
                if (
                    block_idx == extent.block_count - 1
                    and block_used + len(entry_data) <= BLOCK_SIZE
                ):
                    self.image_file.seek(block_num * BLOCK_SIZE + block_used)
                    self.image_file.write(entry_data)
                    self.image_file.flush()
                    return

                current_size += BLOCK_SIZE

        # If we get here, we need to allocate a new block
        for extent_idx, extent in enumerate(dir_inode.extents):
            if extent.block_count == 0:
                new_block = self._allocate_block()
                dir_inode.extents[extent_idx] = Extent(new_block, 1)
                dir_inode.extent_count = max(dir_inode.extent_count, extent_idx + 1)

                # Write new entry to the new block
                self.image_file.seek(new_block * BLOCK_SIZE)
                self.image_file.write(entry_data)
                self.image_file.write(b"\x00" * (BLOCK_SIZE - len(entry_data)))
                self.image_file.flush()

                # Update directory inode
                self._write_inode(dir_inode_num, dir_inode)
                return

        raise OSError("Directory full")

    def _free_inode_blocks(self, inode: Inode):
        """Free all blocks allocated to an inode"""
        for extent in inode.extents:
            if extent.block_count == 0:
                break
            for block_offset in range(extent.block_count):
                block_num = extent.start_block + block_offset
                self._free_block(block_num)
        # Reset extents
        inode.extents = [Extent(0, 0) for _ in range(4)]
        inode.extent_count = 0

    def _free_inode(self, inode_num: int):
        """Free an inode"""
        if inode_num == 0:
            raise ValueError("Invalid inode number 0")

        # Calculate which group contains this inode
        group_num = (inode_num - 1) // INODES_PER_GROUP
        inode_index = (inode_num - 1) % INODES_PER_GROUP

        if group_num >= len(self.group_descriptors):
            raise ValueError(f"Inode {inode_num} is beyond filesystem bounds")

        group_desc = self.group_descriptors[group_num]

        # Read inode bitmap
        self.image_file.seek(group_desc.inode_bitmap_block * BLOCK_SIZE)
        bitmap = bytearray(self.image_file.read(BLOCK_SIZE))

        # Clear inode bit
        byte_idx = inode_index // 8
        bit_idx = inode_index % 8
        bitmap[byte_idx] &= ~(1 << bit_idx)

        # Write bitmap back
        self.image_file.seek(group_desc.inode_bitmap_block * BLOCK_SIZE)
        self.image_file.write(bitmap)

        # Update group descriptor
        group_desc.free_inodes_count += 1
        self.group_descriptors[group_num] = group_desc  # Update in-memory copy
        self._write_group_descriptor(group_num, group_desc)

        # Update superblock
        self.superblock.free_inodes_count += 1
        self._write_superblock()

    def _remove_directory_entry(self, dir_inode_num: int, filename: str):
        """Remove entry from directory"""
        dir_inode = self._get_inode(dir_inode_num)

        if not (dir_inode.mode & S_IFDIR):
            raise OSError("Not a directory")

        # Read directory blocks
        for extent in dir_inode.extents:
            if extent.block_count == 0:
                break

            for block_offset in range(extent.block_count):
                block_num = extent.start_block + block_offset
                self.image_file.seek(block_num * BLOCK_SIZE)
                block_data = bytearray(self.image_file.read(BLOCK_SIZE))

                # Parse directory entries
                offset = 0
                while offset < len(block_data):
                    try:
                        result = DirEntry.unpack(bytes(block_data[offset:]), 0)
                        if result[0] is None:  # Empty entry or end of directory
                            break
                        entry, entry_len = result
                        if entry.name == filename:
                            # Clear the entry by setting inode_num to 0
                            block_data[offset:offset + 4] = b'\x00\x00\x00\x00'
                            self.image_file.seek(block_num * BLOCK_SIZE)
                            self.image_file.write(block_data)
                            self.image_file.flush()
                            return
                        offset += entry_len

                        if entry_len == 0:  # Prevent infinite loop
                            break
                    except (ValueError, UnicodeDecodeError):
                        break

        raise FileNotFoundError(f"No such file or directory: {filename}")

    def _resolve_path(self, path: str) -> int:
        """Resolve path to inode number"""
        if path == "/":
            return ROOT_INODE

        path = path.strip("/")
        components = path.split("/")

        current_inode_num = ROOT_INODE

        for component in components:
            if not component:  # Skip empty components
                continue

            current_inode = self._get_inode(current_inode_num)

            if not (current_inode.mode & S_IFDIR):
                raise OSError(f"Not a directory: {component}")

            found_inode_num = self._find_file_in_directory(current_inode, component)
            if found_inode_num is None:
                raise FileNotFoundError(f"No such file or directory: {component}")

            current_inode_num = found_inode_num

        return current_inode_num

    # Public API methods

    def open(self, path: str, flags: int = O_RDONLY) -> int:
        """Open file and return file descriptor"""
        try:
            inode_num = self._resolve_path(path)
            inode = self._get_inode(inode_num)

            # Check if it's a regular file
            if not (inode.mode & S_IFREG):
                raise OSError("Not a regular file")

        except FileNotFoundError:
            if flags & O_CREAT:
                # Create new file
                parent_path = os.path.dirname(path)
                filename = os.path.basename(path)

                if parent_path == "":
                    parent_path = "/"

                parent_inode_num = self._resolve_path(parent_path)

                # Allocate inode for new file
                inode_num = self._allocate_inode()

                # Create file inode
                current_time = int(time.time())
                inode = Inode(
                    mode=S_IFREG | 0o644,  # Regular file with 644 permissions
                    uid=0,
                    size_lo=0,
                    gid=0,
                    links_count=1,
                    size_high=0,
                    atime=current_time,
                    ctime=current_time,
                    mtime=current_time,
                    flags=0,
                    extent_count=0,
                    extents=[Extent(0, 0) for _ in range(4)],
                )

                self._write_inode(inode_num, inode)

                # Add to parent directory
                self._add_directory_entry(
                    parent_inode_num, filename, inode_num, 1
                )  # Regular file type
            else:
                raise

        # Truncate if requested
        if flags & O_TRUNC:
            inode.size_lo = 0
            inode.size_high = 0
            self._free_inode_blocks(inode)
            self._write_inode(inode_num, inode)

        # Create file descriptor
        fd = self.next_fd
        self.next_fd += 1

        self.open_files[fd] = FileDescriptor(
            inode_num=inode_num, path=path, flags=flags, offset=0, inode=inode
        )

        return fd

    def close(self, fd: int):
        """Close file descriptor"""
        if fd not in self.open_files:
            raise OSError("Bad file descriptor")

        del self.open_files[fd]

    def read(self, fd: int, size: int, offset: Optional[int] = None) -> bytes:
        """Read data from file"""
        if fd not in self.open_files:
            raise OSError("Bad file descriptor")

        file_desc = self.open_files[fd]

        if file_desc.flags & O_WRONLY:
            raise OSError("File not open for reading")

        # Get current inode (in case it was updated)
        inode = self._get_inode(file_desc.inode_num)
        file_size = inode.size_lo | (inode.size_high << 32)

        # Use provided offset or file descriptor offset
        read_offset = offset if offset is not None else file_desc.offset

        if read_offset >= file_size:
            return b""

        # Limit read size to file size
        actual_size = min(size, file_size - read_offset)
        result = bytearray()

        bytes_read = 0
        current_offset = read_offset

        # Read from extents
        for extent in inode.extents:
            if extent.block_count == 0 or bytes_read >= actual_size:
                break

            extent_start_byte = len(result) if len(result) > 0 else 0
            extent_size_bytes = extent.block_count * BLOCK_SIZE

            # Check if we need to read from this extent
            if current_offset < extent_start_byte + extent_size_bytes:
                # Calculate read position within extent
                extent_offset = max(0, current_offset - extent_start_byte)
                bytes_to_read = min(
                    actual_size - bytes_read, extent_size_bytes - extent_offset
                )

                # Read blocks
                start_block = extent.start_block + (extent_offset // BLOCK_SIZE)
                block_offset = extent_offset % BLOCK_SIZE

                while (
                    bytes_to_read > 0
                    and start_block < extent.start_block + extent.block_count
                ):
                    self.image_file.seek(start_block * BLOCK_SIZE + block_offset)
                    chunk_size = min(bytes_to_read, BLOCK_SIZE - block_offset)
                    chunk = self.image_file.read(chunk_size)

                    result.extend(chunk)
                    bytes_read += len(chunk)
                    bytes_to_read -= len(chunk)

                    start_block += 1
                    block_offset = 0

            current_offset = extent_start_byte + extent_size_bytes

        # Update offset if not using explicit offset
        if offset is None:
            file_desc.offset += len(result)

        return bytes(result[:actual_size])

    def write(self, fd: int, data: bytes, offset: Optional[int] = None) -> int:
        """Write data to file"""
        if fd not in self.open_files:
            raise OSError("Bad file descriptor")

        file_desc = self.open_files[fd]

        if file_desc.flags & O_RDONLY:
            raise OSError("File not open for writing")

        # Get current inode
        inode = self._get_inode(file_desc.inode_num)
        file_size = inode.size_lo | (inode.size_high << 32)

        # Use provided offset or file descriptor offset
        write_offset = offset if offset is not None else file_desc.offset

        # Calculate new file size
        new_size = max(file_size, write_offset + len(data))
        blocks_needed = (new_size + BLOCK_SIZE - 1) // BLOCK_SIZE

        # Allocate blocks if needed
        current_blocks = sum(extent.block_count for extent in inode.extents)

        if blocks_needed > current_blocks:
            # Simple allocation - just add to first available extent
            blocks_to_add = blocks_needed - current_blocks

            for extent_idx in range(len(inode.extents)):
                if inode.extents[extent_idx].block_count == 0:
                    # Allocate new extent
                    start_block = self._allocate_block()
                    inode.extents[extent_idx] = Extent(start_block, 1)
                    inode.extent_count = max(inode.extent_count, extent_idx + 1)
                    blocks_to_add -= 1

                    # Allocate additional blocks for this extent
                    for i in range(1, min(blocks_to_add + 1, blocks_needed)):
                        try:
                            next_block = self._allocate_block()
                            if next_block == start_block + i:
                                inode.extents[extent_idx].block_count += 1
                            else:
                                # Non-contiguous, need new extent
                                break
                        except OSError:
                            break

                    break

        # Write data
        bytes_written = 0
        current_offset = write_offset

        for extent in inode.extents:
            if extent.block_count == 0 or bytes_written >= len(data):
                break

            extent_size_bytes = extent.block_count * BLOCK_SIZE

            if current_offset < extent_size_bytes:
                extent_offset = current_offset
                bytes_to_write = min(
                    len(data) - bytes_written, extent_size_bytes - extent_offset
                )

                # Write to blocks
                start_block = extent.start_block + (extent_offset // BLOCK_SIZE)
                block_offset = extent_offset % BLOCK_SIZE

                data_offset = bytes_written

                while (
                    bytes_to_write > 0
                    and start_block < extent.start_block + extent.block_count
                ):
                    chunk_size = min(bytes_to_write, BLOCK_SIZE - block_offset)

                    # Read existing block if partial write
                    if block_offset > 0 or chunk_size < BLOCK_SIZE:
                        self.image_file.seek(start_block * BLOCK_SIZE)
                        block_data = bytearray(self.image_file.read(BLOCK_SIZE))
                        if len(block_data) < BLOCK_SIZE:
                            block_data.extend(b"\x00" * (BLOCK_SIZE - len(block_data)))
                    else:
                        block_data = bytearray(BLOCK_SIZE)

                    # Update block with new data
                    end_offset = min(block_offset + chunk_size, BLOCK_SIZE)
                    chunk_data = data[
                        data_offset : data_offset + (end_offset - block_offset)
                    ]
                    block_data[block_offset:end_offset] = chunk_data

                    # Write block back
                    self.image_file.seek(start_block * BLOCK_SIZE)
                    self.image_file.write(block_data)

                    bytes_written += len(chunk_data)
                    bytes_to_write -= len(chunk_data)
                    data_offset += len(chunk_data)

                    start_block += 1
                    block_offset = 0

            current_offset = extent_size_bytes

        # Update inode size
        inode.size_lo = new_size & 0xFFFFFFFF
        inode.size_high = new_size >> 32
        inode.mtime = int(time.time())
        self._write_inode(file_desc.inode_num, inode)

        # Update offset if not using explicit offset
        if offset is None:
            file_desc.offset += bytes_written

        return bytes_written

    def unlink(self, path: str):
        """Delete file"""
        # Get parent directory and filename
        parent_path = os.path.dirname(path)
        filename = os.path.basename(path)

        if parent_path == "":
            parent_path = "/"

        parent_inode_num = self._resolve_path(parent_path)
        parent_inode = self._get_inode(parent_inode_num)

        if not (parent_inode.mode & S_IFDIR):
            raise OSError("Parent is not a directory")

        # Find file in parent directory
        file_inode_num = self._find_file_in_directory(parent_inode, filename)
        if file_inode_num is None:
            raise FileNotFoundError("No such file or directory")

        file_inode = self._get_inode(file_inode_num)

        # Can only unlink regular files
        if not (file_inode.mode & S_IFREG):
            raise OSError("Not a regular file")

        # Remove from directory
        self._remove_directory_entry(parent_inode_num, filename)
        # Free blocks
        self._free_inode_blocks(file_inode)
        # Free inode
        self._free_inode(file_inode_num)

    def mkdir(self, path: str, mode: int = 0o755):
        """Create directory"""
        parent_path = os.path.dirname(path)
        dirname = os.path.basename(path)

        if parent_path == "":
            parent_path = "/"

        parent_inode_num = self._resolve_path(parent_path)

        # Check if directory already exists
        try:
            self._resolve_path(path)
            raise OSError("Directory already exists")
        except FileNotFoundError:
            pass

        # Allocate inode for new directory
        dir_inode_num = self._allocate_inode()

        # Allocate block for directory entries
        dir_block = self._allocate_block()

        # Create directory inode
        current_time = int(time.time())
        dir_inode = Inode(
            mode=S_IFDIR | mode,
            uid=0,
            size_lo=BLOCK_SIZE,
            gid=0,
            links_count=2,  # . and .. links
            size_high=0,
            atime=current_time,
            ctime=current_time,
            mtime=current_time,
            flags=0,
            extent_count=1,
            extents=[Extent(dir_block, 1), Extent(0, 0), Extent(0, 0), Extent(0, 0)],
        )

        self._write_inode(dir_inode_num, dir_inode)

        # Create . and .. entries
        dot_entry = DirEntry(dir_inode_num, 1, ".", 2)  # Directory type
        dotdot_entry = DirEntry(parent_inode_num, 2, "..", 2)

        # Write directory entries
        self.image_file.seek(dir_block * BLOCK_SIZE)
        self.image_file.write(dot_entry.pack())
        self.image_file.write(dotdot_entry.pack())

        # Zero rest of block
        remaining = BLOCK_SIZE - len(dot_entry.pack()) - len(dotdot_entry.pack())
        self.image_file.write(b"\x00" * remaining)
        self.image_file.flush()

        # Add to parent directory
        self._add_directory_entry(
            parent_inode_num, dirname, dir_inode_num, 2
        )  # Directory type

    def rmdir(self, path: str):
        """Remove empty directory"""
        if path == "/":
            raise OSError("Cannot remove root directory")

        dir_inode_num = self._resolve_path(path)
        dir_inode = self._get_inode(dir_inode_num)

        if not (dir_inode.mode & S_IFDIR):
            raise OSError("Not a directory")

        # Check if directory is empty (only . and .. entries)
        entry_count = 0
        for extent in dir_inode.extents:
            if extent.block_count == 0:
                break

            for block_offset in range(extent.block_count):
                block_num = extent.start_block + block_offset
                self.image_file.seek(block_num * BLOCK_SIZE)
                block_data = self.image_file.read(BLOCK_SIZE)

                offset = 0
                while offset < len(block_data):
                    try:
                        entry, entry_len = DirEntry.unpack(block_data, offset)
                        if entry.name not in [".", ".."]:
                            entry_count += 1
                        offset += entry_len

                        if entry_len == 0:
                            break
                    except (ValueError, UnicodeDecodeError):
                        break

        if entry_count > 0:
            raise OSError("Directory not empty")

        # Get parent directory
        parent_path = os.path.dirname(path)
        dirname = os.path.basename(path)

        if parent_path == "":
            parent_path = "/"

        parent_inode_num = self._resolve_path(parent_path)
        parent_inode = self._get_inode(parent_inode_num)

        # Remove from parent directory
        self._remove_directory_entry(parent_inode_num, dirname)
        # Free blocks
        self._free_inode_blocks(dir_inode)
        # Free inode
        self._free_inode(dir_inode_num)
        # Decrease parent links count
        parent_inode.links_count -= 1
        self._write_inode(parent_inode_num, parent_inode)

    def readdir(self, path: str) -> List[str]:
        """List directory contents"""
        dir_inode_num = self._resolve_path(path)
        dir_inode = self._get_inode(dir_inode_num)

        if not (dir_inode.mode & S_IFDIR):
            raise OSError("Not a directory")

        entries = []

        # Read directory blocks
        for extent in dir_inode.extents:
            if extent.block_count == 0:
                break

            for block_offset in range(extent.block_count):
                block_num = extent.start_block + block_offset
                self.image_file.seek(block_num * BLOCK_SIZE)
                block_data = self.image_file.read(BLOCK_SIZE)

                # Parse directory entries
                offset = 0
                while offset < len(block_data):
                    try:
                        result = DirEntry.unpack(block_data, offset)
                        if result[0] is None:  # Empty entry or end of directory
                            break
                        entry, entry_len = result
                        if entry.name and entry.name not in [".", ".."]:
                            entries.append(entry.name)
                        offset += entry_len

                        if entry_len == 0:
                            break
                    except (ValueError, UnicodeDecodeError):
                        break

        return entries

    def stat(self, path: str) -> Dict[str, Union[int, str]]:
        """Get file/directory metadata"""
        inode_num = self._resolve_path(path)
        inode = self._get_inode(inode_num)

        file_size = inode.size_lo | (inode.size_high << 32)

        return {
            "inode": inode_num,
            "mode": inode.mode,
            "size": file_size,
            "uid": inode.uid,
            "gid": inode.gid,
            "atime": inode.atime,
            "ctime": inode.ctime,
            "mtime": inode.mtime,
            "links_count": inode.links_count,
            "type": "directory" if (inode.mode & S_IFDIR) else "file",
        }

    def close_filesystem(self):
        """Close filesystem"""
        if self.image_file:
            self.image_file.close()
            self.image_file = None


# Global filesystem instance
_fs_instance = None


def init_filesystem(image_path: str) -> FileSystem:
    """Initialize filesystem"""
    global _fs_instance
    _fs_instance = FileSystem(image_path)
    return _fs_instance


def get_filesystem() -> FileSystem:
    """Get current filesystem instance"""
    if _fs_instance is None:
        raise RuntimeError("Filesystem not initialized")
    return _fs_instance


# Convenience functions that mirror the API
def openf(path: str, flags: int = O_RDONLY) -> int:
    return get_filesystem().open(path, flags)


def read(fd: int, size: int, offset: Optional[int] = None) -> bytes:
    return get_filesystem().read(fd, size, offset)


def write(fd: int, data: bytes, offset: Optional[int] = None) -> int:
    return get_filesystem().write(fd, data, offset)


def close(fd: int):
    return get_filesystem().close(fd)


def unlink(path: str):
    return get_filesystem().unlink(path)


def mkdir(path: str, mode: int = 0o755):
    return get_filesystem().mkdir(path, mode)


def rmdir(path: str):
    return get_filesystem().rmdir(path)


def readdir(path: str) -> List[str]:
    return get_filesystem().readdir(path)


def stat(path: str) -> Dict[str, Union[int, str]]:
    return get_filesystem().stat(path)
