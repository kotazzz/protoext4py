import os
import posixpath
import struct
import time
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from fs import INODE_SIZE, Superblock, GroupDesc, Inode
from fs import ExtentHeader, ExtentLeaf, ExtentIndex

# File system constants
BLOCK_SIZE = 4096
BLOCKS_PER_GROUP = 8192
INODES_PER_GROUP = 2048
ROOT_INODE = 2

# File types
S_IFMT   = 0o170000  # битовая маска для типа файла

S_IFSOCK = 0o140000  # сокет
S_IFLNK  = 0o120000  # символьная ссылка
S_IFREG  = 0o100000  # обычный файл
S_IFBLK  = 0o060000  # блочное устройство
S_IFDIR  = 0o040000  # каталог
S_IFCHR  = 0o020000  # символьное устройство
S_IFIFO  = 0o010000  # FIFO / канал

# File flags
O_RDONLY = 0o0  # Read only
O_WRONLY = 0o1  # Write only
O_RDWR = 0o2  # Read/write
O_CREAT = 0o100  # Create if not exists
O_TRUNC = 0o1000  # Truncate to zero length

# Extent tree constants
MAX_LEAF_ENTRIES = (BLOCK_SIZE - 8) // 12  # Max entries in leaf nodes in blocks
MAX_INDEX_ENTRIES = (BLOCK_SIZE - 8) // 12  # Max entries in index nodes in blocks


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
            return None, len(data) - offset  # Return remaining space as empty

        inode_num, entry_len, name_len = struct.unpack(
            "<III", data[offset : offset + 12]
        )

        # Handle empty/end of directory entries
        if inode_num == 0 or entry_len == 0 or name_len == 0:
            return None, max(entry_len, len(data) - offset)

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

        # Mark root directory block as used (in case old image)
        try:
            root_inode = self._get_inode(ROOT_INODE)
            # Try to find the first extent in the B+ tree
            leaf = self._find_extent(root_inode, 0)
            if leaf is not None:
                root_dir_block = leaf.get_start_block()
                group_num = root_dir_block // BLOCKS_PER_GROUP
                if group_num < len(self.group_descriptors):
                    group_desc = self.group_descriptors[group_num]
                    self.image_file.seek(group_desc.block_bitmap_block * BLOCK_SIZE)
                    bitmap = bytearray(self.image_file.read(BLOCK_SIZE))
                    block_idx = root_dir_block % BLOCKS_PER_GROUP
                    byte_idx = block_idx // 8
                    bit_idx = block_idx % 8
                    bitmap[byte_idx] |= (1 << bit_idx)
                    self.image_file.seek(group_desc.block_bitmap_block * BLOCK_SIZE)
                    self.image_file.write(bitmap)
        except Exception:
            pass  # Ignore if fails

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

        inode_data = self.image_file.read(INODE_SIZE)

        if len(inode_data) != INODE_SIZE:
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
        # Only clear if set
        if bitmap[byte_idx] & (1 << bit_idx):
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

    def _allocate_block_at(self, block_num: int):
        """Allocate a specific block by its number"""
        group_num = block_num // BLOCKS_PER_GROUP
        block_idx = block_num % BLOCKS_PER_GROUP

        if group_num >= len(self.group_descriptors):
            raise OSError("Block number out of range")

        group_desc = self.group_descriptors[group_num]

        self.image_file.seek(group_desc.block_bitmap_block * BLOCK_SIZE)
        bitmap = bytearray(self.image_file.read(BLOCK_SIZE))

        byte_idx = block_idx // 8
        bit_idx = block_idx % 8

        if bitmap[byte_idx] & (1 << bit_idx):
            raise OSError(f"Block {block_num} already allocated")

        # Mark block as used
        bitmap[byte_idx] |= 1 << bit_idx

        self.image_file.seek(group_desc.block_bitmap_block * BLOCK_SIZE)
        self.image_file.write(bitmap)

        # Update group descriptor
        group_desc.free_blocks_count -= 1
        self.group_descriptors[group_num] = group_desc
        self._write_group_descriptor(group_num, group_desc)

        # Update superblock
        self.superblock.free_blocks_count -= 1
        self._write_superblock()

    def _update_leaf_in_tree(self, inode_num: int, old_leaf: ExtentLeaf, new_leaf: ExtentLeaf):
        """Update a leaf in the B+ tree"""
        inode = self._get_inode(inode_num)
        self._update_node(inode.extent_root, old_leaf, new_leaf, inode_num)
        # After updating, write back the inode if root changed
        self._write_inode(inode_num, inode)

    def _extend_leaf(self, inode_num: int, leaf: ExtentLeaf):
        """Extend an existing leaf by increasing its block_count"""
        # Allocate the next physical block
        next_physical_block = leaf.get_start_block() + leaf.block_count
        self._allocate_block_at(next_physical_block)

        # Create extended leaf
        extended_leaf = ExtentLeaf(
            logical_block=leaf.logical_block,
            block_count=leaf.block_count + 1,
            start_block_hi=leaf.start_block_hi,
            start_block_lo=leaf.start_block_lo
        )

        # Update in tree
        self._update_leaf_in_tree(inode_num, leaf, extended_leaf)

    def _update_node(self, node_data: bytes, old_leaf: ExtentLeaf, new_leaf: ExtentLeaf, inode_num: int) -> bytes:
        """Recursively update a node in the tree"""
        if len(node_data) < 8:
            return node_data
        
        header = ExtentHeader.unpack(node_data[:8])
        if header.magic != 0xF30A:
            return node_data
        
        entries_data = node_data[8:]
        
        if header.depth == 0:  # Leaf node
            for i in range(header.entries_count):
                leaf_data = entries_data[i*12:(i+1)*12]
                leaf = ExtentLeaf.unpack(leaf_data)
                if leaf.logical_block == old_leaf.logical_block and leaf.get_start_block() == old_leaf.get_start_block():
                    # Found the leaf, update it
                    new_entries = entries_data[:i*12] + new_leaf.pack() + entries_data[(i+1)*12:]
                    return header.pack() + new_entries + b'\x00' * (BLOCK_SIZE - len(header.pack()) - len(new_entries))
            return node_data  # Not found
        else:  # Index node
            for i in range(header.entries_count):
                idx_data = entries_data[i*12:(i+1)*12]
                idx = ExtentIndex.unpack(idx_data)
                # Read child node
                self.image_file.seek(idx.child_block * BLOCK_SIZE)
                child_data = self.image_file.read(BLOCK_SIZE)
                updated_child = self._update_node(child_data, old_leaf, new_leaf, inode_num)
                if updated_child != child_data:
                    # Child was updated, write it back
                    self.image_file.seek(idx.child_block * BLOCK_SIZE)
                    self.image_file.write(updated_child)
                    return node_data  # No change to this node
            return node_data

    def _find_file_in_directory(self, dir_inode: Inode, filename: str) -> Optional[int]:
        """Find file in directory, return inode number"""
        if not ((dir_inode.mode & S_IFMT) == S_IFDIR):
            return None

        # Читаем блоки директории через B+ дерево экстентов
        file_size = dir_inode.size_lo | (dir_inode.size_high << 32)
        bytes_read = 0

        while bytes_read < file_size:
            # Находим экстент для текущего логического блока
            logical_block = bytes_read // BLOCK_SIZE
            leaf = self._find_extent(dir_inode, logical_block)
            if leaf is None:
                break

            # Вычисляем физический блок
            block_offset_in_extent = logical_block - leaf.logical_block
            physical_block = leaf.get_start_block() + block_offset_in_extent

            # Читаем блок
            self.image_file.seek(physical_block * BLOCK_SIZE)
            block_data = self.image_file.read(BLOCK_SIZE)

            # Парсим записи директории
            offset = 0
            while offset + 14 <= len(block_data):
                try:
                    result = DirEntry.unpack(block_data, offset)
                    if result[0] is None:  # Empty entry or end of directory
                        if result[1] == 0:
                            break
                        else:
                            offset += result[1]
                            continue
                    entry, entry_len = result
                    if entry.name == filename:
                        if entry.inode_num == 0:
                            return None  # Skip deleted entries
                        return entry.inode_num
                    offset += entry_len

                    if entry_len == 0:  # Prevent infinite loop
                        break
                except (ValueError, UnicodeDecodeError):
                    break

            bytes_read += BLOCK_SIZE

        return None

    def _add_directory_entry(
        self, dir_inode_num: int, filename: str, file_inode_num: int, file_type: int = 0
    ):
        """Add entry to directory"""
        dir_inode = self._get_inode(dir_inode_num)

        if not ((dir_inode.mode & S_IFMT) == S_IFDIR):
            raise OSError("Not a directory")

        # Create new directory entry
        new_entry = DirEntry(file_inode_num, len(filename), filename, file_type)
        entry_data = new_entry.pack()

        # Ищем место в существующих блоках директории
        file_size = dir_inode.size_lo | (dir_inode.size_high << 32)
        bytes_scanned = 0

        while bytes_scanned < file_size:
            logical_block = bytes_scanned // BLOCK_SIZE
            leaf = self._find_extent(dir_inode, logical_block)
            if leaf is None:
                break

            block_offset_in_extent = logical_block - leaf.logical_block
            physical_block = leaf.get_start_block() + block_offset_in_extent

            self.image_file.seek(physical_block * BLOCK_SIZE)
            block_data = bytearray(self.image_file.read(BLOCK_SIZE))

            # Ищем свободное место в блоке
            offset = 0
            while offset < len(block_data):
                try:
                    result = DirEntry.unpack(bytes(block_data[offset:]), 0)
                    if result[0] is None:  # Empty entry
                        old_entry_len = result[1]
                        new_entry_len = len(entry_data)
                        
                        if old_entry_len >= new_entry_len:
                            remaining_space = old_entry_len - new_entry_len
                            
                            # If remaining space is enough for a new empty entry (at least 12 bytes for header)
                            if remaining_space >= 12:
                                # Split the slot: use part for new entry, create new empty slot for remainder
                                block_data[offset:offset + new_entry_len] = entry_data
                                
                                # Create new empty entry in the remaining space
                                empty_entry_header = struct.pack("<III", 0, remaining_space, 0)
                                block_data[offset + new_entry_len:offset + new_entry_len + 12] = empty_entry_header
                            else:
                                # Not enough space to split, use entire slot
                                block_data[offset:offset + old_entry_len] = entry_data
                                # Update entry_len in the packed data to match the full slot
                                struct.pack_into("<I", block_data, offset + 4, old_entry_len)
                            
                            self.image_file.seek(physical_block * BLOCK_SIZE)
                            self.image_file.write(block_data)
                            self.image_file.flush()
                            return
                    else:
                        entry_len = result[1]
                    offset += entry_len
                    if entry_len == 0:
                        break
                except (ValueError, UnicodeDecodeError):
                    break

            bytes_scanned += BLOCK_SIZE

        # Если не нашли место, выделяем новый блок
        new_block = self._allocate_block()

        # Добавляем новый экстент в дерево
        self._insert_extent(dir_inode_num, ExtentLeaf(
            logical_block=file_size // BLOCK_SIZE,
            block_count=1,
            start_block_hi=(new_block >> 32),
            start_block_lo=(new_block & 0xFFFFFFFF)
        ))

        # Записываем новую запись в новый блок
        self.image_file.seek(new_block * BLOCK_SIZE)
        self.image_file.write(entry_data)
        remaining = BLOCK_SIZE - len(entry_data)
        self.image_file.write(b"\x00" * remaining)
        self.image_file.flush()

        # Обновляем размер директории
        new_size = file_size + BLOCK_SIZE
        dir_inode.size_lo = new_size & 0xFFFFFFFF
        dir_inode.size_high = new_size >> 32
        self._write_inode(dir_inode_num, dir_inode)

    def _free_inode_blocks(self, inode: Inode):
        """Free all blocks allocated to an inode"""
        def free_node_blocks(node_data: bytes):
            """Рекурсивно освобождает блоки из узла дерева"""
            if len(node_data) < 8:
                return
            header = ExtentHeader.unpack(node_data[:8])
            if header.magic != 0xF30A:
                return

            entries_data = node_data[8:]

            if header.depth == 0:  # Листовой узел
                for i in range(header.entries_count):
                    if i * 12 + 12 > len(entries_data):
                        break
                    leaf_data = entries_data[i*12 : (i+1)*12]
                    leaf = ExtentLeaf.unpack(leaf_data)
                    # Освобождаем все блоки в экстенте
                    for block_offset in range(leaf.block_count):
                        block_num = leaf.get_start_block() + block_offset
                        # Skip reserved blocks (0-1 in group 0)
                        group_num = block_num // BLOCKS_PER_GROUP
                        if not (group_num == 0 and block_num < 2):
                            self._free_block(block_num)
            else:  # Индексный узел
                for i in range(header.entries_count):
                    if i * 12 + 12 > len(entries_data):
                        break
                    idx_data = entries_data[i*12 : (i+1)*12]
                    idx = ExtentIndex.unpack(idx_data)
                    # Рекурсивно освобождаем дочерний узел
                    self.image_file.seek(idx.child_block * BLOCK_SIZE)
                    child_data = self.image_file.read(BLOCK_SIZE)
                    free_node_blocks(child_data)
                    # Освобождаем блок самого дочернего узла
                    self._free_block(idx.child_block)

        # Начинаем с корневого узла
        free_node_blocks(inode.extent_root)

        # Сбрасываем дерево экстентов
        header = ExtentHeader(magic=0xF30A, entries_count=0, max_entries=3, depth=0)
        inode.extent_root = header.pack() + b'\x00' * 40

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

        if not ((dir_inode.mode & S_IFMT) == S_IFDIR):
            raise OSError("Not a directory")

        # Read directory blocks through extent tree
        file_size = dir_inode.size_lo | (dir_inode.size_high << 32)
        bytes_read = 0

        while bytes_read < file_size:
            logical_block = bytes_read // BLOCK_SIZE
            leaf = self._find_extent(dir_inode, logical_block)
            if leaf is None:
                break

            block_offset_in_extent = logical_block - leaf.logical_block
            physical_block = leaf.get_start_block() + block_offset_in_extent

            self.image_file.seek(physical_block * BLOCK_SIZE)
            block_data = bytearray(self.image_file.read(BLOCK_SIZE))

            # Parse directory entries
            offset = 0
            prev_entry_offset = -1
            prev_entry_len = 0

            while offset < len(block_data):
                try:
                    # Читаем текущую запись, чтобы получить ее длину
                    result = DirEntry.unpack(bytes(block_data), offset)
                    if result[0] is None:
                        # Дошли до конца или пустой области
                        if result[1] > 0:
                            offset += result[1]
                            continue
                        else:
                            break
                    
                    entry, entry_len = result
                    
                    if entry.name == filename:
                        # Нашли запись для удаления
                        if prev_entry_offset != -1:
                            # Есть предыдущая запись, "поглощаем" текущую
                            # Новая длина предыдущей записи = ее старая длина + длина удаляемой
                            new_prev_len = prev_entry_len + entry_len
                            struct.pack_into("<I", block_data, prev_entry_offset + 4, new_prev_len)
                        else:
                            # Это первая запись в блоке, просто зануляем ее inode
                            struct.pack_into("<I", block_data, offset, 0)
                        
                        # Записываем измененный блок и выходим
                        self.image_file.seek(physical_block * BLOCK_SIZE)
                        self.image_file.write(block_data)
                        return

                    # Запоминаем текущую запись как предыдущую для следующей итерации
                    prev_entry_offset = offset
                    prev_entry_len = entry_len
                    offset += entry_len

                    if entry_len == 0:
                        break
                except (ValueError, UnicodeDecodeError):
                    break

            bytes_read += BLOCK_SIZE

        raise FileNotFoundError(f"No such file or directory: {filename}")

    def _resolve_path(self, path: str, *, follow_links: bool = True, _depth: int = 0) -> int:
        """Resolve path to inode number with symlink depth protection"""
        MAX_SYMLINK_DEPTH = 16
        if _depth > MAX_SYMLINK_DEPTH:
            raise OSError("Too many levels of symbolic links")
        if path == "/":
            return ROOT_INODE

        path = path.strip("/")
        components = path.split("/")

        current_inode_num = ROOT_INODE

        for i, component in enumerate(components):
            if not component:  # Skip empty components
                continue

            current_inode = self._get_inode(current_inode_num)

            if not ((current_inode.mode & S_IFMT) == S_IFDIR):
                raise OSError(f"Not a directory: {component}")

            found_inode_num = self._find_file_in_directory(current_inode, component)
            if found_inode_num is None:
                raise FileNotFoundError(f"No such file or directory: {component}")

            # Check if it's a symlink
            found_inode = self._get_inode(found_inode_num)
            is_last_component = (i == len(components) - 1)
            
            if (found_inode.mode & S_IFMT) == S_IFLNK and (follow_links or not is_last_component):
                # Read the target path
                target_data = b""
                file_size = found_inode.size_lo | (found_inode.size_high << 32)
                
                # Check if it's an inline symlink (no valid extents in extent_root)
                if self._find_extent(found_inode, 0) is None and file_size > 0:
                    # Inline symlink: data is directly in extent_root
                    target_data = found_inode.extent_root[:file_size].rstrip(b'\x00')
                else:
                    # Regular symlink: read data through extent tree
                    bytes_read = 0
                    while bytes_read < file_size:
                        logical_block = bytes_read // BLOCK_SIZE
                        leaf = self._find_extent(found_inode, logical_block)
                        if leaf is None:
                            break
                        block_offset_in_extent = logical_block - leaf.logical_block
                        physical_block = leaf.get_start_block() + block_offset_in_extent
                        self.image_file.seek(physical_block * BLOCK_SIZE)
                        block_data = self.image_file.read(BLOCK_SIZE)
                        null_pos = block_data.find(b"\x00")
                        if null_pos != -1:
                            target_data += block_data[:null_pos]
                            break
                        else:
                            target_data += block_data
                        bytes_read += BLOCK_SIZE
                target_path = target_data.decode('utf-8').strip()
                # Only resolve if target_path looks like a valid path
                if target_path.startswith('/') or not any(c in target_path for c in ['\n', '\r']):
                    # Resolve the target with increased depth
                    found_inode_num = self._resolve_path(target_path, follow_links=True, _depth=_depth + 1)
                else:
                    # This is not a symlink target but file content, treat as regular file
                    pass

            current_inode_num = found_inode_num

        return current_inode_num

    # Public API methods

    def open(self, path: str, flags: int = O_RDONLY) -> int:
        """Open file and return file descriptor"""
        try:
            inode_num = self._resolve_path(path)
            inode = self._get_inode(inode_num)

            # Check if it's a regular file
            if not ((inode.mode & S_IFMT) == S_IFREG):
                raise OSError("Not a regular file")

        except FileNotFoundError:
            if flags & O_CREAT:
                # Create new file
                parent_path = posixpath.dirname(path)
                filename = posixpath.basename(path)

                if parent_path == "":
                    parent_path = "/"

                parent_inode_num = self._resolve_path(parent_path)

                # Allocate inode for new file
                inode_num = self._allocate_inode()

                # Create file inode
                current_time = int(time.time())
                # Инициализация пустого корня дерева экстентов
                header = ExtentHeader(magic=0xF30A, entries_count=0, max_entries=3, depth=0)
                extent_root = header.pack() + b'\x00' * (48 - len(header.pack()))
                inode = Inode(
                    mode=S_IFREG | 0o644,
                    uid=0,
                    size_lo=0,
                    gid=0,
                    links_count=1,
                    size_high=0,
                    atime=current_time,
                    ctime=current_time,
                    mtime=current_time,
                    flags=0,
                    extent_root=extent_root,
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

        file_desc = self.open_files[fd]
        del self.open_files[fd]

        # If inode has no links and no open descriptors, free its resources
        inode_meta = self._get_inode(file_desc.inode_num)
        if inode_meta.links_count == 0:
            still_open = any(f.inode_num == file_desc.inode_num for f in self.open_files.values())
            if not still_open:
                self._free_inode_blocks(inode_meta)
                self._free_inode(file_desc.inode_num)

    def read(self, fd: int, size: int, offset: Optional[int] = None) -> bytes:
        """Read data from file"""
        if fd not in self.open_files:
            raise OSError("Bad file descriptor")

        file_desc = self.open_files[fd]

        if file_desc.flags & O_WRONLY:
            raise OSError("File not open for reading")

        # Get current inode (in case it was updated)
        inode = self._get_inode(file_desc.inode_num)

        # If it's a symlink, read the target path
        if (inode.mode & S_IFMT) == S_IFLNK:
            target_data = b""
            file_size = inode.size_lo | (inode.size_high << 32)
            
            # Check if it's an inline symlink (no valid extents in extent_root)
            if self._find_extent(inode, 0) is None and file_size > 0:
                # Inline symlink: data is directly in extent_root
                target_data = inode.extent_root[:file_size].rstrip(b'\x00')
            else:
                # Regular symlink: read data through extent tree
                bytes_read = 0
                while bytes_read < file_size:
                    logical_block = bytes_read // BLOCK_SIZE
                    leaf = self._find_extent(inode, logical_block)
                    if leaf is None:
                        break
                    block_offset_in_extent = logical_block - leaf.logical_block
                    physical_block = leaf.get_start_block() + block_offset_in_extent
                    self.image_file.seek(physical_block * BLOCK_SIZE)
                    block_data = self.image_file.read(BLOCK_SIZE)
                    null_pos = block_data.find(b"\x00")
                    if null_pos != -1:
                        target_data += block_data[:null_pos]
                        break
                    else:
                        target_data += block_data
                    bytes_read += BLOCK_SIZE
            # Use provided offset or file descriptor offset
            read_offset = offset if offset is not None else file_desc.offset
            if read_offset >= len(target_data):
                return b""
            actual_size = min(size, len(target_data) - read_offset)
            result = target_data[read_offset:read_offset + actual_size]
            # Update offset if not using explicit offset
            if offset is None:
                file_desc.offset += len(result)
            return result
        else:
            # Regular file
            file_size = inode.size_lo | (inode.size_high << 32)

            # Use provided offset or file descriptor offset
            read_offset = offset if offset is not None else file_desc.offset

            if read_offset >= file_size:
                return b""

            # Limit read size to file size
            actual_size = min(size, file_size - read_offset)
            
            result = bytearray()
            bytes_read = 0
            
            while bytes_read < actual_size:
                # Вычисляем логический блок для текущего смещения
                logical_block = (read_offset + bytes_read) // BLOCK_SIZE
                
                # Находим экстент для этого логического блока
                leaf = self._find_extent(inode, logical_block)
                if leaf is None:
                    # Дыра в файле - заполняем нулями
                    hole_size = min(actual_size - bytes_read, BLOCK_SIZE - ((read_offset + bytes_read) % BLOCK_SIZE))
                    result.extend(b'\x00' * hole_size)
                    bytes_read += hole_size
                    continue
                
                # Вычисляем физический блок
                block_offset_in_extent = logical_block - leaf.logical_block
                if block_offset_in_extent >= leaf.block_count:
                    # Вне диапазона экстента
                    hole_size = min(actual_size - bytes_read, BLOCK_SIZE - ((read_offset + bytes_read) % BLOCK_SIZE))
                    result.extend(b'\x00' * hole_size)
                    bytes_read += hole_size
                    continue
                    
                physical_block = leaf.get_start_block() + block_offset_in_extent
                
                # Вычисляем смещение внутри блока
                block_offset = (read_offset + bytes_read) % BLOCK_SIZE
                
                # Определяем, сколько байт можно прочитать из этого блока
                bytes_to_read = min(actual_size - bytes_read, BLOCK_SIZE - block_offset)
                
                # Читаем данные
                self.image_file.seek(physical_block * BLOCK_SIZE + block_offset)
                chunk = self.image_file.read(bytes_to_read)
                result.extend(chunk)
                bytes_read += len(chunk)
            
            # Update offset if not using explicit offset
            if offset is None:
                file_desc.offset += bytes_read
                
            return bytes(result)

    def write(self, fd: int, data: bytes, offset: Optional[int] = None) -> int:
        """Write data to file (fixed: always allocate new block/extents after truncate or for empty file)"""
        if fd not in self.open_files:
            raise OSError("Bad file descriptor")

        file_desc = self.open_files[fd]

        if file_desc.flags == O_RDONLY:
            raise OSError("File not open for writing")

        # Get current inode
        inode = self._get_inode(file_desc.inode_num)
        file_size = inode.size_lo | (inode.size_high << 32)

        # Use provided offset or file descriptor offset
        write_offset = offset if offset is not None else file_desc.offset

        # Calculate new file size
        new_size = max(file_size, write_offset + len(data))

        bytes_written = 0
        data_offset = 0

        while bytes_written < len(data):
            current_offset = write_offset + bytes_written
            logical_block = current_offset // BLOCK_SIZE
            block_offset = current_offset % BLOCK_SIZE

            # Найти экстент для этого логического блока
            leaf = self._find_extent(inode, logical_block)

            if leaf is None:
                # Если файл пустой (или после truncate), всегда выделяем новый блок и экстент
                new_block = self._allocate_block()
                new_leaf = ExtentLeaf(
                    logical_block=logical_block,
                    block_count=1,
                    start_block_hi=(new_block >> 32),
                    start_block_lo=(new_block & 0xFFFFFFFF)
                )
                self._insert_extent(file_desc.inode_num, new_leaf)
                # После вставки перечитываем inode и leaf
                inode = self._get_inode(file_desc.inode_num)
                leaf = self._find_extent(inode, logical_block)
                if leaf is None:
                    break  # что-то пошло не так

            # Вычисляем физический блок
            block_offset_in_extent = logical_block - leaf.logical_block
            if block_offset_in_extent >= leaf.block_count:
                break
            physical_block = leaf.get_start_block() + block_offset_in_extent

            # Читаем существующий блок
            self.image_file.seek(physical_block * BLOCK_SIZE)
            block_data = bytearray(self.image_file.read(BLOCK_SIZE))

            # Записываем данные в блок
            chunk_size = min(len(data) - data_offset, BLOCK_SIZE - block_offset)
            block_data[block_offset:block_offset + chunk_size] = data[data_offset:data_offset + chunk_size]

            # Записываем блок обратно
            self.image_file.seek(physical_block * BLOCK_SIZE)
            self.image_file.write(block_data)

            bytes_written += chunk_size
            data_offset += chunk_size

        # Обновляем метаданные inode
        inode.size_lo = new_size & 0xFFFFFFFF
        inode.size_high = new_size >> 32
        inode.mtime = int(time.time())
        self._write_inode(file_desc.inode_num, inode)

        # Обновляем offset дескриптора
        if offset is None:
            file_desc.offset += bytes_written

        return bytes_written

    def unlink(self, path: str):
        """Delete file"""
        # Get parent directory and filename
        parent_path = posixpath.dirname(path)
        filename = posixpath.basename(path)

        if parent_path == "":
            parent_path = "/"

        parent_inode_num = self._resolve_path(parent_path)
        parent_inode = self._get_inode(parent_inode_num)

        if not ((parent_inode.mode & S_IFMT) == S_IFDIR):
            raise OSError("Parent is not a directory")

        # Find file in parent directory
        file_inode_num = self._find_file_in_directory(parent_inode, filename)
        if file_inode_num is None:
            raise FileNotFoundError("No such file or directory")

        file_inode = self._get_inode(file_inode_num)

        # Can only unlink regular files or symbolic links
        file_type = file_inode.mode & S_IFMT
        if not (file_type == S_IFREG or file_type == S_IFLNK):
            raise OSError("Can only unlink regular files or symbolic links")

        # Remove from directory
        self._remove_directory_entry(parent_inode_num, filename)
        
        # Decrease links count
        file_inode.links_count -= 1

        # Always write the updated inode
        self._write_inode(file_inode_num, file_inode)

        # Check if inode has no links and is not open by any descriptor
        is_open = any(fd.inode_num == file_inode_num for fd in self.open_files.values())

        if file_inode.links_count == 0 and not is_open:
            # Free blocks and inode only when no links and no open descriptors
            self._free_inode_blocks(file_inode)
            self._free_inode(file_inode_num)

    def mkdir(self, path: str, mode: int = 0o755):
        """Create directory"""
        parent_path = posixpath.dirname(path)
        dirname = posixpath.basename(path)

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

        # Create directory inode с инициализцией корня B+ дерева экстентов
        current_time = int(time.time())
        header = ExtentHeader(magic=0xF30A, entries_count=1, max_entries=3, depth=0)
        # Листовой экстент для первого блока директории
        leaf = ExtentLeaf(logical_block=0, block_count=1, start_block_hi=(dir_block >> 32), start_block_lo=(dir_block & 0xFFFFFFFF))
        extent_root = header.pack() + leaf.pack() + b'\x00' * (48 - len(header.pack()) - len(leaf.pack()))
        dir_inode = Inode(
            mode=S_IFDIR | mode,
            uid=0,
            size_lo=BLOCK_SIZE,
            gid=0,
            links_count=2,
            size_high=0,
            atime=current_time,
            ctime=current_time,
            mtime=current_time,
            flags=0,
            extent_root=extent_root,
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
        # Increment parent's link count for new '..' entry
        parent_inode = self._get_inode(parent_inode_num)
        parent_inode.links_count += 1
        self._write_inode(parent_inode_num, parent_inode)

    def rmdir(self, path: str):
        """Remove empty directory"""
        if path == "/":
            raise OSError("Cannot remove root directory")

        dir_inode_num = self._resolve_path(path)
        dir_inode = self._get_inode(dir_inode_num)

        if not ((dir_inode.mode & S_IFMT) == S_IFDIR):
            raise OSError("Not a directory")

        # Check if directory is empty (only . and .. entries)
        entry_count = 0
        file_size = dir_inode.size_lo | (dir_inode.size_high << 32)
        bytes_read = 0

        while bytes_read < file_size:
            logical_block = bytes_read // BLOCK_SIZE
            leaf = self._find_extent(dir_inode, logical_block)
            if leaf is None:
                break

            block_offset_in_extent = logical_block - leaf.logical_block
            physical_block = leaf.get_start_block() + block_offset_in_extent

            self.image_file.seek(physical_block * BLOCK_SIZE)
            block_data = self.image_file.read(BLOCK_SIZE)

            offset = 0
            while offset < len(block_data):
                try:
                    result = DirEntry.unpack(block_data, offset)
                    if result[0] is None:  # Empty entry or end of directory
                        break
                    entry, entry_len = result
                    if entry.name not in [".", ".."]:
                        entry_count += 1
                    offset += entry_len

                    if entry_len == 0:
                        break
                except (ValueError, UnicodeDecodeError):
                    break

            bytes_read += BLOCK_SIZE

        if entry_count > 0:
            raise OSError("Directory not empty")

        # Get parent directory
        parent_path = posixpath.dirname(path)
        dirname = posixpath.basename(path)

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

    def rmdir_recursive(self, path: str):
        """Remove a directory and its contents"""
        dir_inode_num = self._resolve_path(path)
        dir_inode = self._get_inode(dir_inode_num)

        if not ((dir_inode.mode & S_IFMT) == S_IFDIR):
            raise OSError("Not a directory")

        # Recursively remove all entries (skip . and .. entries)
        for entry in self.readdir(path):
            print(entry, path)
            if entry in [".", ".."]:
                continue
            entry_path = posixpath.join(path, entry)
            entry_inode = self._resolve_path(entry_path)
            entry_stat = self._get_inode(entry_inode)

            if entry_stat.mode & S_IFDIR:
                self.rmdir_recursive(entry_path)
            else:
                self.unlink(entry_path)

        # Finally, remove the directory itself
        self.rmdir(path)

    def readdir(self, path: str) -> List[str]:
        """List directory contents"""
        dir_inode_num = self._resolve_path(path)
        dir_inode = self._get_inode(dir_inode_num)

        if not ((dir_inode.mode & S_IFMT) == S_IFDIR):
            raise OSError("Not a directory")

        entries = []

        # Read directory blocks through extent tree
        file_size = dir_inode.size_lo | (dir_inode.size_high << 32)
        bytes_read = 0

        while bytes_read < file_size:
            logical_block = bytes_read // BLOCK_SIZE
            leaf = self._find_extent(dir_inode, logical_block)
            if leaf is None:
                break

            block_offset_in_extent = logical_block - leaf.logical_block
            physical_block = leaf.get_start_block() + block_offset_in_extent

            self.image_file.seek(physical_block * BLOCK_SIZE)
            block_data = self.image_file.read(BLOCK_SIZE)

            # Parse directory entries
            offset = 0
            while offset + 14 <= len(block_data):
                try:
                    result = DirEntry.unpack(block_data, offset)
                    if result[0] is None:  # Empty entry or end of directory
                        if result[1] == 0:
                            break
                        else:
                            offset += result[1]
                            continue
                    entry, entry_len = result
                    if entry.inode_num != 0 and entry.name and entry.name not in [".", ".."]:
                        entries.append(entry.name)
                    offset += entry_len

                    if entry_len == 0:
                        break
                except (ValueError, UnicodeDecodeError):
                    break

            bytes_read += BLOCK_SIZE

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
            "type": inode.mode & S_IFMT,
        }

    def lstat(self, path: str) -> Dict[str, Union[int, str]]:
        """Get file/directory metadata without following symlinks"""
        inode_num = self._resolve_path(path, follow_links=False)
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
            "type": inode.mode & S_IFMT,
        }

    def close_filesystem(self):
        """Close filesystem"""
        if self.image_file:
            self.image_file.close()
            self.image_file = None

    def _find_extent(self, inode: Inode, logical_block: int) -> Optional[ExtentLeaf]:
        """Рекурсивный поиск экстента в B+ дереве."""
        return self._find_extent_in_node(inode.extent_root, logical_block)

    def _find_extent_in_node(self, node_data: bytes, logical_block: int) -> Optional[ExtentLeaf]:
        """Вспомогательная функция для поиска в узле дерева."""
        if len(node_data) < 8:
            return None  # Недостаточно данных для заголовка

        header = ExtentHeader.unpack(node_data[:8])
        if header.magic != 0xF30A:
            # Это не узел дерева, возможно, старый формат или ошибка
            return None

        entries_data = node_data[8:]  # Данные после заголовка

        if header.depth == 0:  # Листовой узел
            for i in range(header.entries_count):
                if i * 12 + 12 > len(entries_data):
                    break
                leaf_data = entries_data[i*12 : (i+1)*12]
                leaf = ExtentLeaf.unpack(leaf_data)
                if leaf.logical_block <= logical_block < leaf.logical_block + leaf.block_count:
                    return leaf
        else:  # Индексный узел
            # Линейный поиск (для простоты; в реальности можно оптимизировать бинарным поиском)
            next_child_block = 0
            for i in range(header.entries_count):
                if i * 12 + 12 > len(entries_data):
                    break
                idx_data = entries_data[i*12 : (i+1)*12]
                idx = ExtentIndex.unpack(idx_data)
                if logical_block >= idx.logical_block:
                    next_child_block = idx.child_block
                else:
                    break  # Нашли нужный диапазон

            if next_child_block == 0:
                return None  # Не нашли подходящий дочерний узел

            # Читаем дочерний узел с диска и рекурсивно ищем в нем
            self.image_file.seek(next_child_block * BLOCK_SIZE)
            child_node_data = self.image_file.read(BLOCK_SIZE)
            return self._find_extent_in_node(child_node_data, logical_block)

        return None

    def _insert_extent(self, inode_num: int, new_leaf: ExtentLeaf):
        """Вставка нового экстента в B+ дерево"""
        inode = self._get_inode(inode_num)

        # Для простоты начнем с корневого узла
        root_data = bytearray(inode.extent_root)

        if len(root_data) < 8:
            # Пустой корень, инициализируем
            header = ExtentHeader(magic=0xF30A, entries_count=1, max_entries=3, depth=0)
            root_data = header.pack() + new_leaf.pack() + b'\x00' * (48 - len(header.pack()) - len(new_leaf.pack()))
            inode.extent_root = bytes(root_data)
            self._write_inode(inode_num, inode)
        else:
            # Пытаемся вставить в существующий узел
            success, new_index = self._insert_into_node(root_data, new_leaf, inode_num, -1)
            if success:
                # Вставка удалась, обновляем корень в иноде
                inode.extent_root = bytes(root_data)
                self._write_inode(inode_num, inode)
            else:
                if new_index is None:
                    # Корень полон, разделяем его
                    self._split_root(inode_num, new_leaf)
                    # _split_root уже обновил inode.extent_root, ничего не делаем
                else:
                    # Для индексного корня, разделяем его
                    self._split_root(inode_num, new_index)
                    # _split_root уже обновил inode.extent_root, ничего не делаем

    def _insert_into_node(self, node_data: bytearray, new_entry: Union[ExtentLeaf, ExtentIndex], inode_num: int, block_num: int = -1) -> Tuple[bool, Optional[ExtentIndex]]:
        """Вставка в узел, возвращает (success, new_index_if_split)"""
        header = ExtentHeader.unpack(node_data[:8])

        if header.depth == 0:  # Листовой узел
            if header.entries_count >= header.max_entries:
                return False, None  # Полон, нужно разделить
            entries_data = node_data[8:8 + header.entries_count * 12]
            # Находим место для вставки (сортировка по logical_block)
            insert_pos = 0
            for i in range(header.entries_count):
                leaf_data = entries_data[i*12 : (i+1)*12]
                leaf = ExtentLeaf.unpack(leaf_data)
                if leaf.logical_block > new_entry.logical_block:
                    break
                insert_pos = i + 1
            # Вставляем
            new_entries = entries_data[:insert_pos*12] + new_entry.pack() + entries_data[insert_pos*12 : header.entries_count*12]
            # Заполняем до максимального размера нулями
            max_size = header.max_entries * 12
            if len(new_entries) < max_size:
                new_entries += b'\x00' * (max_size - len(new_entries))
            elif len(new_entries) > max_size:
                new_entries = new_entries[:max_size]
            node_data[8:8 + max_size] = new_entries
            header.entries_count += 1
            node_data[:8] = header.pack()
            if block_num != -1:
                self.image_file.seek(block_num * BLOCK_SIZE)
                self.image_file.write(node_data)
            return True, None
        else:
            # Индексный узел - рекурсивно спускаемся
            entries_data = node_data[8:]
            # Ищем подходящий дочерний узел
            next_child_block = 0
            for i in range(header.entries_count):
                idx_data = entries_data[i*12:(i+1)*12]
                idx = ExtentIndex.unpack(idx_data)
                if new_entry.logical_block >= idx.logical_block:
                    next_child_block = idx.child_block
                else:
                    break
            if next_child_block == 0:
                return False, None  # Не нашли

            # Читаем дочерний узел
            self.image_file.seek(next_child_block * BLOCK_SIZE)
            child_data = bytearray(self.image_file.read(BLOCK_SIZE))
            success, new_index = self._insert_into_node(child_data, new_entry, inode_num, next_child_block)
            if success:
                # Рекурсивный вызов уже записал обновленный дочерний узел на диск
                return True, None
            else:
                if new_index is None:
                    # Листовой узел полон, разделяем его
                    new_index = self._split_leaf_node(next_child_block, bytes(child_data), new_entry)
                # Теперь вставляем new_index в текущий индексный узел
                if header.entries_count >= header.max_entries:
                    # Текущий индексный узел тоже полон, нужно разделить его
                    return False, new_index
                # Вставляем new_index
                insert_pos = 0
                for i in range(header.entries_count):
                    idx_data = entries_data[i*12:(i+1)*12]
                    idx = ExtentIndex.unpack(idx_data)
                    if idx.logical_block > new_index.logical_block:
                        break
                    insert_pos = i + 1
                new_entries = entries_data[:insert_pos*12] + new_index.pack() + entries_data[insert_pos*12:header.entries_count*12]
                max_size = header.max_entries * 12
                if len(new_entries) < max_size:
                    new_entries += b'\x00' * (max_size - len(new_entries))
                node_data[8:8 + max_size] = new_entries
                header.entries_count += 1
                node_data[:8] = header.pack()
                if block_num != -1:
                    self.image_file.seek(block_num * BLOCK_SIZE)
                    self.image_file.write(node_data)
                return True, None

    def _find_path(self, inode_num: int, logical_block: int) -> List[Tuple[int, bytes]]:
        """Find path to leaf node containing logical_block, return list of (block_num, node_data)"""
        path = []
        inode = self._get_inode(inode_num)
        current_data = inode.extent_root
        current_block = -1  # Special value for root in inode
        
        while True:
            path.append((current_block, current_data))
            if len(current_data) < 8:
                break
            header = ExtentHeader.unpack(current_data[:8])
            if header.magic != 0xF30A:
                break
            if header.depth == 0:
                break  # Leaf node
            entries_data = current_data[8:]
            next_child_block = 0
            for i in range(header.entries_count):
                if i * 12 + 12 > len(entries_data):
                    break
                idx_data = entries_data[i*12:(i+1)*12]
                idx = ExtentIndex.unpack(idx_data)
                if logical_block >= idx.logical_block:
                    next_child_block = idx.child_block
                else:
                    break
            if next_child_block == 0:
                break
            # Read child node
            self.image_file.seek(next_child_block * BLOCK_SIZE)
            current_data = self.image_file.read(BLOCK_SIZE)
            current_block = next_child_block
        return path

    def _split_root(self, inode_num: int, new_entry: Union[ExtentLeaf, ExtentIndex]):
        """Разделение корневого узла"""
        inode = self._get_inode(inode_num)

        # Выделяем два новых блока для дочерних узлов
        left_block = self._allocate_block()
        right_block = self._allocate_block()

        # Читаем текущий корень
        root_data = bytearray(inode.extent_root)
        header = ExtentHeader.unpack(root_data[:8])
        entries_data = root_data[8:]

        # Собираем все экстенты: старые + новый
        all_entries = []
        for i in range(header.entries_count):
            leaf_data = entries_data[i*12:(i+1)*12]
            leaf = ExtentLeaf.unpack(leaf_data)
            all_entries.append(leaf)
        all_entries.append(new_entry)
        all_entries.sort(key=lambda x: x.logical_block)

        # Разделяем на две половины
        mid = len(all_entries) // 2
        left_entries = all_entries[:mid]
        right_entries = all_entries[mid:]

        # Создаем левый листовой узел
        left_header = ExtentHeader(magic=0xF30A, entries_count=len(left_entries), max_entries=MAX_LEAF_ENTRIES, depth=0)
        left_data = left_header.pack()
        for leaf in left_entries:
            left_data += leaf.pack()
        left_data += b'\x00' * (BLOCK_SIZE - len(left_data))

        # Создаем правый листовой узел
        right_header = ExtentHeader(magic=0xF30A, entries_count=len(right_entries), max_entries=MAX_LEAF_ENTRIES, depth=0)
        right_data = right_header.pack()
        for leaf in right_entries:
            right_data += leaf.pack()
        right_data += b'\x00' * (BLOCK_SIZE - len(right_data))

        # Записываем дочерние узлы
        self.image_file.seek(left_block * BLOCK_SIZE)
        self.image_file.write(left_data)
        self.image_file.seek(right_block * BLOCK_SIZE)
        self.image_file.write(right_data)

        # Создаем новый корень как индексный узел
        root_header = ExtentHeader(magic=0xF30A, entries_count=2, max_entries=3, depth=1)
        left_idx = ExtentIndex(logical_block=left_entries[0].logical_block, child_block=left_block)
        right_idx = ExtentIndex(logical_block=right_entries[0].logical_block, child_block=right_block)
        root_data = root_header.pack() + left_idx.pack() + right_idx.pack() + b'\x00' * (BLOCK_SIZE - len(root_header.pack()) - len(left_idx.pack()) - len(right_idx.pack()))

        inode.extent_root = root_data[:48]  # Ограничиваем 48 байтами
        self._write_inode(inode_num, inode)

    def _split_leaf_node(self, node_block: int, node_data: bytes, new_leaf: ExtentLeaf) -> ExtentIndex:
        """Разделение листового узла, возвращает новую индексную запись для родителя"""
        # Собираем все старые экстенты + новый
        header = ExtentHeader.unpack(node_data[:8])
        entries_data = node_data[8:]
        all_entries = []
        for i in range(header.entries_count):
            leaf_data = entries_data[i*12:(i+1)*12]
            leaf = ExtentLeaf.unpack(leaf_data)
            all_entries.append(leaf)
        all_entries.append(new_leaf)
        all_entries.sort(key=lambda x: x.logical_block)

        # Разделяем на две половины
        mid = len(all_entries) // 2
        left_entries = all_entries[:mid]
        right_entries = all_entries[mid:]

        # Выделяем новый блок для правой половины
        right_block = self._allocate_block()

        # Создаем левый узел (обновляем существующий)
        left_header = ExtentHeader(magic=0xF30A, entries_count=len(left_entries), max_entries=MAX_LEAF_ENTRIES, depth=0)
        left_data = left_header.pack()
        for leaf in left_entries:
            left_data += leaf.pack()
        left_data += b'\x00' * (BLOCK_SIZE - len(left_data))

        # Создаем правый узел
        right_header = ExtentHeader(magic=0xF30A, entries_count=len(right_entries), max_entries=MAX_LEAF_ENTRIES, depth=0)
        right_data = right_header.pack()
        for leaf in right_entries:
            right_data += leaf.pack()
        right_data += b'\x00' * (BLOCK_SIZE - len(right_data))

        # Записываем левый узел (обновляем существующий)
        self.image_file.seek(node_block * BLOCK_SIZE)
        self.image_file.write(left_data)

        # Записываем правый узел
        self.image_file.seek(right_block * BLOCK_SIZE)
        self.image_file.write(right_data)

        # Возвращаем индексную запись для правого узла
        return ExtentIndex(logical_block=right_entries[0].logical_block, child_block=right_block)

    def _split_index_node(self, node_block: int, node_data: bytes, new_index: ExtentIndex) -> Tuple[ExtentIndex, bytes]:
        """Разделение индексного узла, возвращает (поднятый индекс, данные нового правого узла)"""
        header = ExtentHeader.unpack(node_data[:8])
        entries_data = node_data[8:]
        all_indices = []
        for i in range(header.entries_count):
            idx_data = entries_data[i*12:(i+1)*12]
            idx = ExtentIndex.unpack(idx_data)
            all_indices.append(idx)
        all_indices.append(new_index)
        all_indices.sort(key=lambda x: x.logical_block)

        # Для индексных узлов, средний индекс поднимается наверх
        mid = len(all_indices) // 2
        left_indices = all_indices[:mid]
        right_indices = all_indices[mid+1:]
        promoted_index = all_indices[mid]

        # Выделяем новый блок для правой половины
        right_block = self._allocate_block()

        # Создаем левый узел (обновляем существующий)
        left_header = ExtentHeader(magic=0xF30A, entries_count=len(left_indices), max_entries=MAX_INDEX_ENTRIES, depth=header.depth)
        left_data = left_header.pack()
        for idx in left_indices:
            left_data += idx.pack()
        left_data += b'\x00' * (BLOCK_SIZE - len(left_data))

        # Создаем правый узел
        right_header = ExtentHeader(magic=0xF30A, entries_count=len(right_indices), max_entries=MAX_INDEX_ENTRIES, depth=header.depth)
        right_data = right_header.pack()
        for idx in right_indices:
            right_data += idx.pack()
        right_data += b'\x00' * (BLOCK_SIZE - len(right_data))

        # Записываем левый узел
        self.image_file.seek(node_block * BLOCK_SIZE)
        self.image_file.write(left_data)

        # Записываем правый узел
        self.image_file.seek(right_block * BLOCK_SIZE)
        self.image_file.write(right_data)

        # Возвращаем поднятый индекс и данные правого узла
        promoted_index.child_block = right_block  # Обновляем child_block на правый блок
        return promoted_index, right_data


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


def lstat(path: str) -> Dict[str, Union[int, str]]:
    return get_filesystem().lstat(path)
