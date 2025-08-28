import struct
import attr

from crc32 import crc32

INODE_SIZE = 88  # Новый размер инода с B+ деревом экстентов: 12 байт заголовка + 36 байт записей = 48 байт, + 40 байт базовых полей = 88

@attr.s(auto_attribs=True)
class Extent:
    start_block: int
    block_count: int

    def pack(self) -> bytes:
        return struct.pack("<QI", self.start_block, self.block_count)

    @classmethod
    def unpack(cls, data: bytes) -> "Extent":
        return cls(*struct.unpack("<QI", data))
 
@attr.s(auto_attribs=True)
class ExtentHeader:
    """Заголовок узла B+ дерева экстентов"""
    magic: int         # 0xF30A
    entries_count: int # число записей в узле
    max_entries: int   # максимальное число записей
    depth: int         # глубина дерева (0 - лист)

    def pack(self) -> bytes:
        return struct.pack("<HHHH", self.magic, self.entries_count, self.max_entries, self.depth)

    @classmethod
    def unpack(cls, data: bytes) -> "ExtentHeader":
        magic, entries_count, max_entries, depth = struct.unpack("<HHHH", data[:8])
        return cls(magic, entries_count, max_entries, depth)

@attr.s(auto_attribs=True)
class ExtentIndex:
    """Запись в индексном узле B+ дерева экстентов"""
    logical_block: int  # первый логический блок
    child_block: int    # физический номер блока дочернего узла

    def pack(self) -> bytes:
        return struct.pack("<IQ", self.logical_block, self.child_block)

    @classmethod
    def unpack(cls, data: bytes) -> "ExtentIndex":
        logical_block, child_block = struct.unpack("<IQ", data[:12])
        return cls(logical_block, child_block)

@attr.s(auto_attribs=True)
class ExtentLeaf:
    """Запись в листовом узле B+ дерева экстентов (12 байт)"""
    logical_block: int  # первый логический блок в экстенте (4 байта)
    block_count: int    # количество блоков в экстенте (2 байта)
    start_block_hi: int # старшие 16 бит первого физического блока
    start_block_lo: int # младшие 32 бит первого физического блока

    def pack(self) -> bytes:
        # структура: logical_block(4) + block_count(2) + start_block_hi(2) + start_block_lo(4)
        return struct.pack("<IHHI", self.logical_block, self.block_count, self.start_block_hi, self.start_block_lo)

    @classmethod
    def unpack(cls, data: bytes) -> "ExtentLeaf":
        logical_block, block_count, start_block_hi, start_block_lo = struct.unpack("<IHHI", data[:12])
        return cls(logical_block, block_count, start_block_hi, start_block_lo)

    def get_start_block(self) -> int:
        return (self.start_block_hi << 32) | self.start_block_lo


@attr.s(auto_attribs=True)
class Inode:
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
    # Корень B+ дерева экстентов: первые 12 байт - заголовок, оставшиеся 36 - записи
    extent_root: bytes = attr.ib(default=b'\x00' * 48)

    def pack(self) -> bytes:
        # Pack базовые поля
        base_tuple = (
            self.mode,
            self.uid,
            self.size_lo,
            self.gid,
            self.links_count,
            self.size_high,
            self.atime,
            self.ctime,
            self.mtime,
            self.flags,
        )
        data = struct.pack("<IIIIIIIIII", *base_tuple)
        # Добавляем сырые 48 байт корня дерева экстентов
        return data + self.extent_root

    @classmethod
    def unpack(cls, data: bytes) -> "Inode":
        # Распаковываем базовые поля
        main_data = data[:40]
        fields = struct.unpack("<IIIIIIIIII", main_data)
        # Сырые 48 байт корня дерева экстентов
        extent_root = data[40:88]
        return cls(*fields, extent_root)


@attr.s(auto_attribs=True)
class GroupDesc:
    block_bitmap_block: int
    inode_bitmap_block: int
    inode_table_block: int
    free_blocks_count: int
    free_inodes_count: int

    def pack(self) -> bytes:
        return struct.pack("<QQQII", self.block_bitmap_block, self.inode_bitmap_block, self.inode_table_block, self.free_blocks_count, self.free_inodes_count)

    @classmethod
    def unpack(cls, data: bytes) -> "GroupDesc":
        return cls(*struct.unpack("<QQQII", data))


@attr.s(auto_attribs=True)
class Superblock:
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
            self.fs_size_blocks,
            self.block_size,
            self.blocks_per_group,
            self.inodes_per_group,
            self.total_inodes,
            self.free_blocks_count,
            self.free_inodes_count,
            self.first_data_block,
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
