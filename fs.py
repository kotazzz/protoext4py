import struct
import attr

from crc32 import crc32

class Packable:
    _fmt: str

    def pack(self) -> bytes:
        return struct.pack(self._fmt, *attr.astuple(self, filter=lambda a, v: a.init))

    @classmethod
    def unpack(cls, data: bytes) -> "Packable":
        return cls(*struct.unpack(cls._fmt, data))

@attr.s(auto_attribs=True)
class Extent(Packable):
    _fmt: str = attr.ib(init=False, default="<QI")
    start_block: int
    block_count: int

@attr.s(auto_attribs=True)
class Inode(Packable):
    # Format: <HHIIQIIIHI (fields) + 4x(QI) (4 extents)
    _fmt: str = attr.ib(init=False, default="<HHIIQIIIHI" + "QI"*4)
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
    _fmt: str = attr.ib(init=False, default="<QQQII")
    block_bitmap_block: int
    inode_bitmap_block: int
    inode_table_block: int
    free_blocks_count: int
    free_inodes_count: int

@attr.s(auto_attribs=True)
class Superblock(Packable):
    _fmt: str = attr.ib(init=False, default="<QIIQQQQI")
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
        return crc32(data + b"\x00"*4)
    
    def pack(self) -> bytes:
        data = super().pack()
        checksum = self.calc_checksum(data)
        self.checksum = checksum
        packed_checksum = struct.pack("<I", checksum)
        return data + packed_checksum
    
