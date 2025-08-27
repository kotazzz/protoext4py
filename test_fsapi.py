#!/usr/bin/env python3
"""
Test script for filesystem API
"""

from fsapi import create_filesystem_api
from main import mkfs

def test_basic_operations():
    """Test basic filesystem operations"""
    image_path = "test_fs.img"
    
    # Create filesystem
    print("Creating filesystem...")
    mkfs(image_path)
    
    # Mount filesystem
    print("Mounting filesystem...")
    fs = create_filesystem_api(image_path)
    
    try:
        # Test stat on root directory
        print("\n=== Testing stat on root directory ===")
        root_stat = fs.stat("/")
        if root_stat:
            print("Root directory stats:")
            print(f"  Size: {root_stat['size']}")
            print(f"  Mode: {oct(root_stat['mode'])}")
            print(f"  Type: {root_stat['type']}")
            print(f"  Links: {root_stat['links']}")
        else:
            print("Failed to stat root directory")
        
        # Test opening root directory
        print("\n=== Testing open root directory ===")
        root_fd = fs.open("/")
        if root_fd >= 0:
            print(f"Successfully opened root directory, fd: {root_fd}")
            fs.close(root_fd)
            print("Closed root directory")
        else:
            print("Failed to open root directory")
        
        # Test creating a new file
        print("\n=== Testing file creation ===")
        from fsapi import O_CREAT, O_RDWR
        file_fd = fs.open("testfile", O_CREAT | O_RDWR)
        if file_fd >= 0:
            print(f"Successfully created file, fd: {file_fd}")
            
            # Test writing to file
            print("\n=== Testing file write ===")
            test_data = b"Hello, filesystem!"
            bytes_written = fs.write(file_fd, test_data)
            if bytes_written > 0:
                print(f"Successfully wrote {bytes_written} bytes")
                
                # Test reading from file
                print("\n=== Testing file read ===")
                read_data = fs.read(file_fd, len(test_data), 0)
                print(f"Read data: {read_data}")
                if read_data == test_data:
                    print("✓ Read data matches written data")
                else:
                    print("✗ Read data doesn't match written data")
            else:
                print("Failed to write to file")
            
            fs.close(file_fd)
            print("Closed test file")
        else:
            print("Failed to create file")
        
        # Test creating a directory
        print("\n=== Testing directory creation ===")
        result = fs.mkdir("testdir")
        if result == 0:
            print("Successfully created directory 'testdir'")
        else:
            print("Failed to create directory")
    
    finally:
        # Unmount filesystem
        fs.unmount()
        print("\nFilesystem unmounted")

def test_multiple_files():
    """Test working with multiple files"""
    image_path = "test_fs.img" 
    fs = create_filesystem_api(image_path)
    
    try:
        from fsapi import O_CREAT, O_RDWR
        
        print("\n=== Testing multiple file operations ===")
        
        # Open multiple files
        files = []
        for i in range(3):
            fd = fs.open(f"file{i}", O_CREAT | O_RDWR)
            if fd >= 0:
                files.append(fd)
                test_data = f"Content of file {i}".encode()
                fs.write(fd, test_data)
                print(f"Created and wrote to file{i}, fd: {fd}")
        
        # Read back from all files
        for i, fd in enumerate(files):
            data = fs.read(fd, 100, 0)  # Read up to 100 bytes from start
            print(f"file{i} contains: {data.decode()}")
            fs.close(fd)
        
        print("All files processed successfully")
        
    finally:
        fs.unmount()

if __name__ == "__main__":
    test_basic_operations()
    test_multiple_files()
