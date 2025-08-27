import os
import struct
from fs import Extent, Superblock, GroupDesc, Inode

# CONSTANTS
BLOCK_SIZE = 4096
INODE_SIZE = 96  # Updated to match actual Inode structure size
BLOCKS_PER_GROUP = 8192  # 32MB per group
INODES_PER_GROUP = 2048

def create_empty_image(image_path: str, size_mb: int = 100):
    """Create an empty image file"""
    size_bytes = size_mb * 1024 * 1024
    with open(image_path, "wb") as f:
        f.write(b'\x00' * size_bytes)
    print(f"Created empty image {image_path} ({size_mb}MB)")

def mkfs(image_path: str):
    """Initialize ext4-like filesystem in the image file"""
    if not os.path.exists(image_path):
        create_empty_image(image_path)
    
    size = os.path.getsize(image_path)
    block_count = size // BLOCK_SIZE
    num_groups = (block_count + BLOCKS_PER_GROUP - 1) // BLOCKS_PER_GROUP
    total_inodes = num_groups * INODES_PER_GROUP
    
    print("Initializing filesystem:")
    print(f"  Size: {size} bytes ({size // (1024*1024)}MB)")
    print(f"  Block size: {BLOCK_SIZE}")
    print(f"  Total blocks: {block_count}")
    print(f"  Block groups: {num_groups}")
    print(f"  Total inodes: {total_inodes}")
    
    with open(image_path, "r+b") as f:
        # Step 1: Create and write superblock
        create_superblock(f, block_count, num_groups, total_inodes)
        
        # Step 2: Create block groups
        create_block_groups(f, num_groups)
        
        # Step 3: Create root inode
        create_root_inode(f)
        
        print("Filesystem initialized successfully!")

def create_superblock(f, block_count: int, num_groups: int, total_inodes: int):
    """Create and write superblock to the image"""
    # Reserve blocks for metadata (superblock + group descriptors + bitmaps + inode tables)
    reserved_blocks = 1  # superblock
    reserved_blocks += (num_groups * 32 + BLOCK_SIZE - 1) // BLOCK_SIZE  # group descriptors
    reserved_blocks += num_groups * 3  # block bitmap + inode bitmap + inode table per group
    
    superblock = Superblock(
        fs_size_blocks=block_count,
        block_size=BLOCK_SIZE,
        blocks_per_group=BLOCKS_PER_GROUP,
        inodes_per_group=INODES_PER_GROUP,
        total_inodes=total_inodes,
        free_blocks_count=block_count - reserved_blocks - 1,  # -1 for root directory block
        free_inodes_count=total_inodes - 1,  # -1 for root inode
        first_data_block=1,
    )
    
    # Write superblock at offset 0
    f.seek(0)
    superblock_data = superblock.pack()
    f.write(superblock_data)
    
    print(f"Superblock written ({len(superblock_data)} bytes)")

def create_block_groups(f, num_groups: int):
    """Create block group descriptors and initialize bitmaps"""
    current_block = 1  # Start after superblock
    
    # Reserve space for group descriptors
    group_desc_blocks = (num_groups * 32 + BLOCK_SIZE - 1) // BLOCK_SIZE
    current_block += group_desc_blocks
    
    group_descriptors = []
    
    for group_num in range(num_groups):
        # Calculate block positions for this group
        block_bitmap_block = current_block
        inode_bitmap_block = current_block + 1
        inode_table_block = current_block + 2
        
        # Inode table size (in blocks)
        inodes_per_block = BLOCK_SIZE // INODE_SIZE
        inode_table_blocks = (INODES_PER_GROUP + inodes_per_block - 1) // inodes_per_block
        
        current_block += 3 + inode_table_blocks  # bitmaps + inode table
        
        # Create group descriptor
        group_desc = GroupDesc(
            block_bitmap_block=block_bitmap_block,
            inode_bitmap_block=inode_bitmap_block,
            inode_table_block=inode_table_block,
            free_blocks_count=BLOCKS_PER_GROUP - (3 + inode_table_blocks),
            free_inodes_count=INODES_PER_GROUP - (1 if group_num == 0 else 0)  # Reserve root inode in group 0
        )
        group_descriptors.append(group_desc)
        
        # Initialize block bitmap (all free)
        f.seek(block_bitmap_block * BLOCK_SIZE)
        f.write(b'\x00' * BLOCK_SIZE)
        
        # Initialize inode bitmap (all free, except root inode in group 0)
        f.seek(inode_bitmap_block * BLOCK_SIZE)
        if group_num == 0:
            # Mark inode 2 (root) as used - bit 1 (0-indexed, since inode numbering starts from 1)
            bitmap = bytearray(BLOCK_SIZE)
            bitmap[0] = 0x02  # Set bit 1 (for inode #2)
            f.write(bitmap)
        else:
            f.write(b'\x00' * BLOCK_SIZE)
        
        # Initialize inode table (all zeros)
        f.seek(inode_table_block * BLOCK_SIZE)
        f.write(b'\x00' * (inode_table_blocks * BLOCK_SIZE))
        
        print(f"Group {group_num}: bitmap_block={block_bitmap_block}, inode_bitmap={inode_bitmap_block}, inode_table={inode_table_block}")
    
    # Write group descriptors
    f.seek(BLOCK_SIZE)  # After superblock
    for group_desc in group_descriptors:
        f.write(group_desc.pack())
    
    print(f"Created {num_groups} block groups")

def create_root_inode(f):
    """Create root directory inode (inode #2)"""
    # Root inode is at position 1 in the inode table (0-indexed, so inode #2)
    # Group 0's inode table starts after superblock + group descriptors + bitmaps
    
    # Find group 0 descriptor to get inode table location
    f.seek(BLOCK_SIZE)  # Group descriptors start after superblock
    group_desc_data = f.read(32)  # Read first group descriptor
    
    if len(group_desc_data) < 32:
        raise ValueError("Could not read group descriptor")
    
    group_desc = GroupDesc.unpack(group_desc_data)
    
    # Root inode (inode #2) position in inode table
    root_inode_offset = group_desc.inode_table_block * BLOCK_SIZE + 1 * INODE_SIZE  # inode #2 is at index 1
    
    # Create root directory inode
    root_inode = Inode(
        mode=0o040755,  # Directory with 755 permissions (S_IFDIR | 0755)
        uid=0,         # Root user
        size_lo=BLOCK_SIZE,  # Size of directory block
        gid=0,         # Root group
        links_count=2, # . and .. links
        size_high=0,
        atime=0,       # Access time (could use current time)
        ctime=0,       # Creation time
        mtime=0,       # Modification time
        flags=0,
        extent_count=1,
        extents=[
            Extent(start_block=group_desc.inode_table_block + ((INODES_PER_GROUP * INODE_SIZE + BLOCK_SIZE - 1) // BLOCK_SIZE), 
                   block_count=1),  # One block for root directory
            Extent(0, 0),
            Extent(0, 0),
            Extent(0, 0)
        ]
    )
    
    # Write root inode
    f.seek(root_inode_offset)
    f.write(root_inode.pack())
    
    # Initialize root directory block with . and .. entries
    root_dir_block = root_inode.extents[0].start_block
    f.seek(root_dir_block * BLOCK_SIZE)
    
    # Create . entry (points to itself - inode 2)
    dot_entry = struct.pack('<III', 2, 16, 1) + b'\x02\x00' + b'.' + b'\x00' * 1  # 16 bytes total
    
    # Create .. entry (points to itself for root - inode 2) 
    dotdot_entry = struct.pack('<III', 2, 16, 2) + b'\x02\x00' + b'..' + b'\x00' * 0  # 16 bytes total
    
    # Write directory entries
    dir_data = dot_entry + dotdot_entry + b'\x00' * (BLOCK_SIZE - 32)  # Fill rest with zeros
    f.write(dir_data)
    f.flush()
    
    print(f"Root inode created at offset {root_inode_offset}")
    print(f"Root directory block: {root_dir_block}")

def main():
    image_path = "fs.img"
    mkfs(image_path)

if __name__ == "__main__":
    main()
