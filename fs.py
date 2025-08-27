import struct
import attr

from crc32 import crc32

class Packable:
    _fmt: str

    def pack(self) -> bytes:
        return struct.pack(self._fmt, *(attr.astuple(self)))

    @classmethod
    def unpack(cls, data: bytes) -> "Packable":
        return cls(*struct.unpack(cls._fmt, data))

@attr.s(auto_attribs=True)
class Extent(Packable):
    _fmt = "<QI"
    start_block: int
    block_count: int

@attr.s(auto_attribs=True)
class Inode(Packable):
    _fmt = "<" + "I"*11 + "QI"*4  # 11 32-bit fields + 4 extents (each QI = 64+32 bit)
    mode: int
    uid: int
    size_lo: int
    gid: int
    links_count: int
    size_high: int
    atime: int
    ctime: int
    mtime: int
    flags: int
    extent_count: int
    extents: list[Extent] = attr.ib(factory=lambda: [Extent(0,0) for _ in range(4)])

    def pack(self) -> bytes:
        # Pack all fields except extents
        base_tuple = (
            self.mode, self.uid, self.size_lo, self.gid, self.links_count,
            self.size_high, self.atime, self.ctime, self.mtime, self.flags, self.extent_count
        )
        # Flatten extents
        ext_tuple = []
        for ext in self.extents:
            ext_tuple.extend([ext.start_block, ext.block_count])
        return struct.pack(self._fmt, *(base_tuple + tuple(ext_tuple)))

    @classmethod
    def unpack(cls, data: bytes) -> "Inode":
        unpacked = struct.unpack(cls._fmt, data)
        base_fields = unpacked[:11]
        extents_raw = unpacked[11:]
        extents = [Extent(extents_raw[i], extents_raw[i+1]) for i in range(0, 8, 2)]
        return cls(*base_fields, extents)

@attr.s(auto_attribs=True)
class GroupDesc(Packable):
    _fmt = "<QQQII"
    block_bitmap_block: int
    inode_bitmap_block: int
    inode_table_block: int
    free_blocks_count: int
    free_inodes_count: int

@attr.s(auto_attribs=True)
class Superblock(Packable):
    _fmt = "<QIIQQQQI"
    fs_size_blocks: int
    block_size: int
    blocks_per_group: int
    inodes_per_group: int
    total_inodes: int
    free_blocks_count: int
    free_inodes_count: int
    first_data_block: int
    checksum: int = attr.ib(init=False, default=0)
    
    def calc_checksum(self, data: bytes) -> int:
        return crc32(data)
    
    def pack(self) -> bytes:
        # Pack all fields except checksum
        base_tuple = (
            self.fs_size_blocks, self.block_size, self.blocks_per_group,
            self.inodes_per_group, self.total_inodes, self.free_blocks_count,
            self.free_inodes_count, self.first_data_block
        )
        data = struct.pack("<QIIQQQQI", *base_tuple)
        
        # Calculate and append checksum
        checksum = self.calc_checksum(data)
        self.checksum = checksum
        return data + struct.pack("<I", checksum)
    
    @classmethod
    def unpack(cls, data: bytes) -> "Superblock":
        # Unpack the main fields (52 bytes for the 8 fields in _fmt)
        main_data = data[:52]
        unpacked = struct.unpack("<QIIQQQQI", main_data)
        
        # Extract checksum if present (last 4 bytes)
        if len(data) >= 56:
            checksum_data = data[52:56]
            checksum = struct.unpack("<I", checksum_data)[0]
        else:
            checksum = 0
            
        # Create superblock instance
        superblock = cls(*unpacked)
        superblock.checksum = checksum
        return superblock
    
