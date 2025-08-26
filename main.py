import os
from fs import Extent, Superblock

# CONST

def mkfs(image_path: str):
    size = os.path.getsize(image_path)
    BLOCK_SIZE = 4096
    BLOCK_COUNT = size // BLOCK_SIZE
    INODES_PER_BLOCK = BLOCK_SIZE // 128
    TOTAL_INODES = BLOCK_COUNT * INODES_PER_BLOCK
    
    # with open(image_path, "wb") as f:
    superblock = Superblock(
                fs_size_blocks=BLOCK_COUNT,
                block_size=BLOCK_SIZE,
                blocks_per_group=BLOCK_COUNT,
                inodes_per_group=32,
                total_inodes=TOTAL_INODES,
                free_blocks_count=BLOCK_COUNT - 1,
                free_inodes_count=TOTAL_INODES - 1,
                first_data_block=1,
        )
    print(superblock.pack())

def main():
    mkfs("fs.img")


if __name__ == "__main__":
    main()
