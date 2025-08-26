def crc32(data: bytes) -> int:
    crc = 0
    for byte in data:
        crc = crc ^ byte
    return crc
