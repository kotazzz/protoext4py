import os
import struct
from fs import INODE_SIZE, Superblock, GroupDesc, Inode
from fsapi import BLOCK_SIZE, BLOCKS_PER_GROUP, INODES_PER_GROUP


def create_empty_image(image_path: str, size_mb: int = 100):
    """Create an empty image file"""
    size_bytes = size_mb * 1024 * 1024
    with open(image_path, "wb") as f:
        f.write(b"\x00" * size_bytes)
    # print(f"Created empty image {image_path} ({size_mb}MB)")


def mkfs(image_path: str):
    """Initialize ext4-like filesystem in the image file"""
    if not os.path.exists(image_path):
        create_empty_image(image_path)

    size = os.path.getsize(image_path)
    block_count = size // BLOCK_SIZE
    num_groups = (block_count + BLOCKS_PER_GROUP - 1) // BLOCKS_PER_GROUP
    total_inodes = num_groups * INODES_PER_GROUP

    # print("Initializing filesystem:")
    # print(f"  Size: {size} bytes ({size // (1024 * 1024)}MB)")
    # print(f"  Block size: {BLOCK_SIZE}")
    # print(f"  Total blocks: {block_count}")
    # print(f"  Block groups: {num_groups}")
    # print(f"  Total inodes: {total_inodes}")

    with open(image_path, "r+b") as f:
        # Step 1: Create and write superblock
        create_superblock(f, block_count, num_groups, total_inodes)

        # Step 2: Create block groups
        create_block_groups(f, num_groups, block_count)

        # Step 3: Create root inode
        create_root_inode(f)

        # print("Filesystem initialized successfully!")


def create_superblock(f, block_count: int, num_groups: int, total_inodes: int):
    """Create and write superblock to the image"""
    # Reserve blocks for metadata (superblock + group descriptors + bitmaps + inode tables)
    reserved_blocks = 1  # superblock
    reserved_blocks += (
        num_groups * 32 + BLOCK_SIZE - 1
    ) // BLOCK_SIZE  # group descriptors
    reserved_blocks += (
        num_groups * 3
    )  # block bitmap + inode bitmap per group
    inodes_per_block = BLOCK_SIZE // INODE_SIZE
    inode_table_blocks = (INODES_PER_GROUP + inodes_per_block - 1) // inodes_per_block
    reserved_blocks += num_groups * inode_table_blocks  # inode tables

    superblock = Superblock(
        fs_size_blocks=block_count,
        block_size=BLOCK_SIZE,
        blocks_per_group=BLOCKS_PER_GROUP,
        inodes_per_group=INODES_PER_GROUP,
        total_inodes=total_inodes,
        free_blocks_count=block_count
        - reserved_blocks
        - 1,  # -1 for root directory block
        free_inodes_count=total_inodes - 1,  # -1 for root inode
        first_data_block=1,
    )

    # Write superblock at offset 0
    f.seek(0)
    superblock_data = superblock.pack()
    f.write(superblock_data)

    # print(f"Superblock written ({len(superblock_data)} bytes)")


def create_block_groups(f, num_groups: int, block_count: int):
    """Create block group descriptors and initialize bitmaps"""
    group_descriptors = []

    inodes_per_block = BLOCK_SIZE // INODE_SIZE
    inode_table_blocks = (INODES_PER_GROUP + inodes_per_block - 1) // inodes_per_block

    for group_num in range(num_groups):
        if group_num == 0:
            block_bitmap_block = 2
            inode_bitmap_block = 3
            inode_table_block = 4
        else:
            group_start_block = group_num * BLOCKS_PER_GROUP
            block_bitmap_block = group_start_block
            inode_bitmap_block = group_start_block + 1
            inode_table_block = group_start_block + 2

        if group_num == 0:
            pass
        else:
            pass

        blocks_in_group = min(BLOCKS_PER_GROUP, block_count - group_num * BLOCKS_PER_GROUP)
        free_blocks_count = blocks_in_group - (3 + inode_table_blocks)
        if group_num == 0:
            free_blocks_count -= 2  # sb and gd

        # Create group descriptor
        group_desc = GroupDesc(
            block_bitmap_block=block_bitmap_block,
            inode_bitmap_block=inode_bitmap_block,
            inode_table_block=inode_table_block,
            free_blocks_count=free_blocks_count,
            free_inodes_count=INODES_PER_GROUP - (1 if group_num == 0 else 0),
        )
        group_descriptors.append(group_desc)

        # Initialize block bitmap
        f.seek(block_bitmap_block * BLOCK_SIZE)
        bitmap = bytearray(BLOCK_SIZE)
        
        # Mark metadata blocks as used
        metadata_blocks = [block_bitmap_block, inode_bitmap_block]
        metadata_blocks.extend(range(inode_table_block, inode_table_block + inode_table_blocks))
        if group_num == 0:
            metadata_blocks.extend([0, 1])
        
        for block in metadata_blocks:
            if block // BLOCKS_PER_GROUP == group_num:
                block_idx = block % BLOCKS_PER_GROUP
                byte_idx = block_idx // 8
                bit_idx = block_idx % 8
                bitmap[byte_idx] |= (1 << bit_idx)
        
        f.write(bitmap)

        # Initialize inode bitmap
        f.seek(inode_bitmap_block * BLOCK_SIZE)
        if group_num == 0:
            bitmap = bytearray(BLOCK_SIZE)
            bitmap[0] = 0x02  # inode 2
            f.write(bitmap)
        else:
            f.write(b"\x00" * BLOCK_SIZE)

        # Initialize inode table
        f.seek(inode_table_block * BLOCK_SIZE)
        f.write(b"\x00" * (inode_table_blocks * BLOCK_SIZE))

        # print(f"Group {group_num}: bitmap_block={block_bitmap_block}, inode_bitmap={inode_bitmap_block}, inode_table={inode_table_block}, blocks_in_group={blocks_in_group}, free_blocks={free_blocks_count}")

    # Write group descriptors
    f.seek(BLOCK_SIZE)  # After superblock
    for group_desc in group_descriptors:
        f.write(group_desc.pack())

    # print(f"Created {num_groups} block groups")


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
    root_inode_offset = (
        group_desc.inode_table_block * BLOCK_SIZE + 1 * INODE_SIZE
    )  # inode #2 is at index 1

    # Calculate root directory block
    root_dir_block = group_desc.inode_table_block + ((INODES_PER_GROUP * INODE_SIZE + BLOCK_SIZE - 1) // BLOCK_SIZE)

    # Create root directory inode with extent tree
    from fs import ExtentHeader, ExtentLeaf
    header = ExtentHeader(magic=0xF30A, entries_count=1, max_entries=3, depth=0)
    leaf = ExtentLeaf(
        logical_block=0,
        block_count=1,
        start_block_hi=(root_dir_block >> 32),
        start_block_lo=(root_dir_block & 0xFFFFFFFF)
    )
    extent_root = header.pack() + leaf.pack() + b'\x00' * (48 - len(header.pack()) - len(leaf.pack()))

    root_inode = Inode(
        mode=0o040755,  # Directory with 755 permissions (S_IFDIR | 0755)
        uid=0,  # Root user
        size_lo=BLOCK_SIZE,  # Size of directory block
        gid=0,  # Root group
        links_count=2,  # . and .. links
        size_high=0,
        atime=0,  # Access time (could use current time)
        ctime=0,  # Creation time
        mtime=0,  # Modification time
        flags=0,
        extent_root=extent_root,
    )

    # Write root inode
    f.seek(root_inode_offset)
    f.write(root_inode.pack())

    # Initialize root directory block with . and .. entries
    f.seek(root_dir_block * BLOCK_SIZE)

    # Create . entry (points to itself - inode 2)
    name_bytes = b"."
    entry_len = 14 + len(name_bytes)
    entry_len = ((entry_len + 3) // 4) * 4
    dot_entry = struct.pack("<III", 2, entry_len, 1) + bytes([2]) + b"\x00" + name_bytes + b"\x00" * (entry_len - 14 - len(name_bytes))

    # Create .. entry (points to itself for root - inode 2)
    name_bytes = b".."
    entry_len = 14 + len(name_bytes)
    entry_len = ((entry_len + 3) // 4) * 4
    dotdot_entry = struct.pack("<III", 2, entry_len, 2) + bytes([2]) + b"\x00" + name_bytes + b"\x00" * (entry_len - 14 - len(name_bytes))

    # Write directory entries
    dir_data = dot_entry + dotdot_entry + b"\x00" * (BLOCK_SIZE - len(dot_entry) - len(dotdot_entry))
    f.write(dir_data)
    f.flush()

    # Mark root_dir_block as used in bitmap
    f.seek(2 * BLOCK_SIZE)  # block_bitmap_block for group 0
    bitmap = bytearray(f.read(BLOCK_SIZE))
    block_idx = root_dir_block % BLOCKS_PER_GROUP
    byte_idx = block_idx // 8
    bit_idx = block_idx % 8
    bitmap[byte_idx] |= (1 << bit_idx)
    f.seek(2 * BLOCK_SIZE)
    f.write(bitmap)

    # Update group descriptor free_blocks_count
    f.seek(BLOCK_SIZE)
    group_desc_data = f.read(32)
    group_desc = GroupDesc.unpack(group_desc_data)
    group_desc.free_blocks_count -= 1
    f.seek(BLOCK_SIZE)
    f.write(group_desc.pack())

    # Update superblock free_blocks_count
    f.seek(0)
    sb_data = f.read(56)
    superblock = Superblock.unpack(sb_data)
    superblock.free_blocks_count -= 1
    f.seek(0)
    f.write(superblock.pack())

    # print(f"Root inode created at offset {root_inode_offset}")
    # print(f"Root directory block: {root_dir_block}")


def main():
    image_path = "fs.img"
    mkfs(image_path)


if __name__ == "__main__":
    main()
