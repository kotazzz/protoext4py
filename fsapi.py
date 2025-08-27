from typing import Optional, List
from fs import Superblock, GroupDesc, Inode, Extent

# Constants
BLOCK_SIZE = 4096
INODE_SIZE = 128
BLOCKS_PER_GROUP = 8192
INODES_PER_GROUP = 2048

# File flags
O_RDONLY = 0x0
O_WRONLY = 0x1
O_RDWR = 0x2
O_CREAT = 0x40
O_TRUNC = 0x200
O_APPEND = 0x400

# File types
S_IFREG = 0o100000  # Regular file
S_IFDIR = 0o40000   # Directory

class FileDescriptor:
    """Represents an open file descriptor"""
    def __init__(self, inode_num: int, inode: Inode, flags: int):
        self.inode_num = inode_num
        self.inode = inode
        self.flags = flags
        self.pos = 0

class FileSystemAPI:
    """Basic filesystem API implementation"""
    
    def __init__(self, image_path: str):
        self.image_path = image_path
        self.image_file = None
        self.superblock = None
        self.group_descriptors = []
        self.open_files = {}  # fd -> FileDescriptor
        self.next_fd = 3  # Start from 3 (0,1,2 are stdin,stdout,stderr)
        
    def mount(self):
        """Mount the filesystem"""
        self.image_file = open(self.image_path, "r+b")
        
        # Read superblock
        self.image_file.seek(0)
        superblock_data = self.image_file.read(56)  # Read 56 bytes (52 for fields + 4 for checksum)
        self.superblock = Superblock.unpack(superblock_data)
        
        # Read group descriptors
        num_groups = (self.superblock.fs_size_blocks + BLOCKS_PER_GROUP - 1) // BLOCKS_PER_GROUP
        self.image_file.seek(BLOCK_SIZE)  # After superblock
        for i in range(num_groups):
            group_data = self.image_file.read(32)
            if len(group_data) == 32:
                self.group_descriptors.append(GroupDesc.unpack(group_data))
    
    def unmount(self):
        """Unmount the filesystem"""
        if self.image_file:
            self.image_file.close()
            self.image_file = None
    
    def _find_inode(self, inode_num: int) -> Optional[Inode]:
        """Find and read inode by number"""
        if inode_num < 1:
            return None
            
        # Calculate which group contains this inode
        group_num = (inode_num - 1) // INODES_PER_GROUP
        inode_index = (inode_num - 1) % INODES_PER_GROUP
        
        if group_num >= len(self.group_descriptors):
            return None
            
        group_desc = self.group_descriptors[group_num]
        inode_offset = group_desc.inode_table_block * BLOCK_SIZE + inode_index * INODE_SIZE
        
        self.image_file.seek(inode_offset)
        inode_data = self.image_file.read(INODE_SIZE)
        
        if len(inode_data) != INODE_SIZE:
            return None
            
        try:
            return Inode.unpack(inode_data)
        except Exception:
            return None
    
    def _write_inode(self, inode_num: int, inode: Inode):
        """Write inode to disk"""
        group_num = (inode_num - 1) // INODES_PER_GROUP
        inode_index = (inode_num - 1) % INODES_PER_GROUP
        
        group_desc = self.group_descriptors[group_num]
        inode_offset = group_desc.inode_table_block * BLOCK_SIZE + inode_index * INODE_SIZE
        
        self.image_file.seek(inode_offset)
        self.image_file.write(inode.pack())
    
    def _allocate_inode(self) -> Optional[int]:
        """Allocate a new inode"""
        # Simple allocation - find first free inode
        for group_num, group_desc in enumerate(self.group_descriptors):
            if group_desc.free_inodes_count > 0:
                # Read inode bitmap
                self.image_file.seek(group_desc.inode_bitmap_block * BLOCK_SIZE)
                bitmap = bytearray(self.image_file.read(BLOCK_SIZE))
                
                # Find free bit
                for byte_idx in range(len(bitmap)):
                    if bitmap[byte_idx] != 0xFF:
                        for bit_idx in range(8):
                            if not (bitmap[byte_idx] & (1 << bit_idx)):
                                # Mark as used
                                bitmap[byte_idx] |= (1 << bit_idx)
                                
                                # Write back bitmap
                                self.image_file.seek(group_desc.inode_bitmap_block * BLOCK_SIZE)
                                self.image_file.write(bitmap)
                                
                                # Update group descriptor
                                group_desc.free_inodes_count -= 1
                                self.image_file.seek(BLOCK_SIZE + group_num * 32)
                                self.image_file.write(group_desc.pack())
                                
                                return group_num * INODES_PER_GROUP + byte_idx * 8 + bit_idx + 1
        return None
    
    def _allocate_block(self) -> Optional[int]:
        """Allocate a new data block"""
        # Simple allocation - find first free block
        for group_num, group_desc in enumerate(self.group_descriptors):
            if group_desc.free_blocks_count > 0:
                # Read block bitmap
                self.image_file.seek(group_desc.block_bitmap_block * BLOCK_SIZE)
                bitmap = bytearray(self.image_file.read(BLOCK_SIZE))
                
                # Find free bit
                for byte_idx in range(len(bitmap)):
                    if bitmap[byte_idx] != 0xFF:
                        for bit_idx in range(8):
                            if not (bitmap[byte_idx] & (1 << bit_idx)):
                                # Mark as used
                                bitmap[byte_idx] |= (1 << bit_idx)
                                
                                # Write back bitmap
                                self.image_file.seek(group_desc.block_bitmap_block * BLOCK_SIZE)
                                self.image_file.write(bitmap)
                                
                                # Update group descriptor
                                group_desc.free_blocks_count -= 1
                                self.image_file.seek(BLOCK_SIZE + group_num * 32)
                                self.image_file.write(group_desc.pack())
                                
                                return group_num * BLOCKS_PER_GROUP + byte_idx * 8 + bit_idx
        return None
    
    def open(self, path: str, flags: int = O_RDONLY) -> int:
        """Open a file and return file descriptor"""
        # For now, implement simple case - assume path is just filename
        # In full implementation, would need path resolution
        
        if path == "/" or path == "":
            # Root directory - inode 2
            root_inode = self._find_inode(2)
            if root_inode:
                fd = self.next_fd
                self.next_fd += 1
                self.open_files[fd] = FileDescriptor(2, root_inode, flags)
                return fd
            return -1
        
        # For create flag, allocate new inode
        if flags & O_CREAT:
            inode_num = self._allocate_inode()
            if not inode_num:
                return -1  # No free inodes
                
            # Create new regular file inode
            new_inode = Inode(
                mode=S_IFREG | 0o644,
                uid=0,
                size_lo=0,
                gid=0,
                links_count=1,
                size_high=0,
                atime=0,
                ctime=0,
                mtime=0,
                flags=0,
                extent_count=0,
                extents=[Extent(0, 0) for _ in range(4)]
            )
            
            self._write_inode(inode_num, new_inode)
            
            fd = self.next_fd
            self.next_fd += 1
            self.open_files[fd] = FileDescriptor(inode_num, new_inode, flags)
            return fd
        
        # TODO: Implement proper path lookup
        return -1  # File not found
    
    def read(self, fd: int, size: int, offset: Optional[int] = None) -> bytes:
        """Read data from file"""
        if fd not in self.open_files:
            return b""
        
        file_desc = self.open_files[fd]
        if offset is not None:
            file_desc.pos = offset
        
        # Read from extents
        data = b""
        remaining = size
        current_pos = file_desc.pos
        
        for extent in file_desc.inode.extents:
            if extent.block_count == 0:
                break
                
            extent_size = extent.block_count * BLOCK_SIZE
            
            if current_pos >= extent_size:
                current_pos -= extent_size
                continue
            
            # Read from this extent
            block_offset = current_pos // BLOCK_SIZE
            byte_offset = current_pos % BLOCK_SIZE
            
            for block_idx in range(block_offset, extent.block_count):
                if remaining <= 0:
                    break
                    
                block_addr = (extent.start_block + block_idx) * BLOCK_SIZE + byte_offset
                self.image_file.seek(block_addr)
                
                to_read = min(remaining, BLOCK_SIZE - byte_offset)
                block_data = self.image_file.read(to_read)
                data += block_data
                
                remaining -= len(block_data)
                byte_offset = 0  # Only first block has offset
                current_pos = 0
        
        file_desc.pos += len(data)
        return data
    
    def write(self, fd: int, data: bytes, offset: Optional[int] = None) -> int:
        """Write data to file"""
        if fd not in self.open_files:
            return -1
        
        file_desc = self.open_files[fd]
        if offset is not None:
            file_desc.pos = offset
        
        # For simplicity, allocate new block if needed
        if file_desc.inode.extent_count == 0:
            block_num = self._allocate_block()
            if not block_num:
                return -1
            
            file_desc.inode.extents[0] = Extent(block_num, 1)
            file_desc.inode.extent_count = 1
        
        # Write to first extent (simplified)
        extent = file_desc.inode.extents[0]
        write_pos = extent.start_block * BLOCK_SIZE + file_desc.pos
        
        self.image_file.seek(write_pos)
        bytes_written = len(data)
        self.image_file.write(data)
        
        # Update inode size
        new_size = file_desc.pos + bytes_written
        file_desc.inode.size_lo = new_size
        self._write_inode(file_desc.inode_num, file_desc.inode)
        
        file_desc.pos += bytes_written
        return bytes_written
    
    def close(self, fd: int) -> int:
        """Close file descriptor"""
        if fd in self.open_files:
            del self.open_files[fd]
            return 0
        return -1
    
    def unlink(self, path: str) -> int:
        """Delete a file"""
        # TODO: Implement file deletion
        # Would need to:
        # 1. Find inode by path
        # 2. Free data blocks
        # 3. Free inode
        # 4. Remove directory entry
        return -1  # Not implemented
    
    def mkdir(self, path: str, mode: int = 0o755) -> int:
        """Create directory"""
        inode_num = self._allocate_inode()
        if not inode_num:
            return -1
        
        # Create directory inode
        dir_inode = Inode(
            mode=S_IFDIR | mode,
            uid=0,
            size_lo=BLOCK_SIZE,
            gid=0,
            links_count=2,  # . and ..
            size_high=0,
            atime=0,
            ctime=0,
            mtime=0,
            flags=0,
            extent_count=1,
            extents=[Extent(0, 0) for _ in range(4)]
        )
        
        # Allocate block for directory
        block_num = self._allocate_block()
        if not block_num:
            return -1
        
        dir_inode.extents[0] = Extent(block_num, 1)
        self._write_inode(inode_num, dir_inode)
        
        # Initialize directory block (empty for now)
        self.image_file.seek(block_num * BLOCK_SIZE)
        self.image_file.write(b'\x00' * BLOCK_SIZE)
        
        return 0
    
    def rmdir(self, path: str) -> int:
        """Remove directory"""
        # TODO: Implement directory removal
        return -1
    
    def readdir(self, path: str) -> List[str]:
        """List directory contents"""
        # TODO: Implement directory reading
        # Would need to parse directory entries
        return []
    
    def stat(self, path: str) -> Optional[dict]:
        """Get file/directory metadata"""
        if path == "/" or path == "":
            root_inode = self._find_inode(2)
            if root_inode:
                return {
                    'size': root_inode.size_lo,
                    'mode': root_inode.mode,
                    'uid': root_inode.uid,
                    'gid': root_inode.gid,
                    'atime': root_inode.atime,
                    'mtime': root_inode.mtime,
                    'ctime': root_inode.ctime,
                    'links': root_inode.links_count,
                    'type': 'directory' if root_inode.mode & S_IFDIR else 'file'
                }
        
        # TODO: Implement path lookup for other files
        return None

# Convenience functions
def create_filesystem_api(image_path: str) -> FileSystemAPI:
    """Create and mount filesystem API"""
    fs_api = FileSystemAPI(image_path)
    fs_api.mount()
    return fs_api