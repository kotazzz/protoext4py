#!/usr/bin/env python3
"""
Test script for the filesystem API
"""

import os
from main import mkfs
import fsapi

def test_basic_operations():
    """Test basic filesystem operations"""
    
    # Create filesystem
    image_path = "test_fs.img"
    if os.path.exists(image_path):
        os.remove(image_path)
    
    print("=== Creating filesystem ===")
    mkfs(image_path)
    
    # Initialize filesystem API
    print("\n=== Initializing filesystem API ===")
    fs = fsapi.init_filesystem(image_path)
    
    try:
        # Test stat on root directory
        print("\n=== Testing stat on root directory ===")
        root_stat = fsapi.stat('/')
        print(f"Root directory stats: {root_stat}")
        
        # Test readdir on root (should be empty initially)
        print("\n=== Testing readdir on empty root ===")
        root_files = fsapi.readdir('/')
        print(f"Root directory contents: {root_files}")
        
        # Test creating a directory
        print("\n=== Testing mkdir ===")
        fsapi.mkdir('/testdir', 0o755)
        print("Created directory /testdir")
        
        # Check root directory now contains the new directory
        root_files = fsapi.readdir('/')
        print(f"Root directory contents after mkdir: {root_files}")
        
        # Test creating a file
        print("\n=== Testing file creation and writing ===")
        fd = fsapi.openf('/testfile.txt', fsapi.O_CREAT | fsapi.O_WRONLY)
        print(f"Created file, got fd: {fd}")
        
        # Write some data
        test_data = b"Hello, World! This is a test file.\nSecond line.\n"
        bytes_written = fsapi.write(fd, test_data)
        print(f"Wrote {bytes_written} bytes")
        
        fsapi.close(fd)
        print("File closed")
        
        # Check directory contents after file creation
        print("Root directory contents after file creation:", fsapi.readdir('/'))

        # Test reading the file
        print("\n=== Testing file reading ===")
        fd = fsapi.openf('/testfile.txt', fsapi.O_RDONLY)
        read_data = fsapi.read(fd, 1024)
        print(f"Read {len(read_data)} bytes: {read_data.decode('utf-8', errors='replace')}")
        fsapi.close(fd)
        
        # Test file stats
        print("\n=== Testing file stat ===")
        file_stat = fsapi.stat('/testfile.txt')
        print(f"File stats: {file_stat}")
        
        # Check root directory contents again
        root_files = fsapi.readdir('/')
        print(f"Root directory contents after file creation: {root_files}")
        
        # Test creating file in subdirectory
        print("\n=== Testing file creation in subdirectory ===")
        fd = fsapi.openf('/testdir/subfile.txt', fsapi.O_CREAT | fsapi.O_WRONLY)
        subfile_data = b"This is a file in subdirectory"
        fsapi.write(fd, subfile_data)
        fsapi.close(fd)
        
        # List subdirectory contents
        subdir_files = fsapi.readdir('/testdir')
        print(f"Subdirectory contents: {subdir_files}")
        
        # Test truncating file
        print("\n=== Testing file truncation ===")
        fd = fsapi.openf('/testfile.txt', fsapi.O_WRONLY | fsapi.O_TRUNC)
        new_data = b"Truncated and new content"
        fsapi.write(fd, new_data)
        fsapi.close(fd)
        
        # Read truncated file
        fd = fsapi.openf('/testfile.txt', fsapi.O_RDONLY)
        truncated_data = fsapi.read(fd, 1024)
        print(f"After truncation: {truncated_data.decode('utf-8', errors='replace')}")
        fsapi.close(fd)
        
        print("\n=== All tests completed successfully! ===")
        
    except Exception as e:
        print(f"Error during testing: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        fs.close_filesystem()

def test_error_conditions():
    """Test error conditions"""
    
    image_path = "test_fs.img"
    fs = fsapi.init_filesystem(image_path)
    
    try:
        print("\n=== Testing error conditions ===")
        
        # Test opening non-existent file without O_CREAT
        try:
            fd = fsapi.openf('/nonexistent.txt', fsapi.O_RDONLY)
            print("ERROR: Should have failed!")
        except FileNotFoundError as e:
            print(f"✓ Correctly caught FileNotFoundError: {e}")
        
        # Test reading from write-only file
        fd = None
        try:
            fd = fsapi.openf('/testfile.txt', fsapi.O_WRONLY)
            fsapi.read(fd, 100)  # This should fail
            print("ERROR: Should have failed!")
        except OSError as e:
            print(f"✓ Correctly caught OSError for read on write-only file: {e}")
        except FileNotFoundError as e:
            print(f"✓ Correctly caught FileNotFoundError: {e}")
        finally:
            if fd is not None:
                fsapi.close(fd)
        
        # Test mkdir on existing directory
        try:
            fsapi.mkdir('/testdir', 0o755)
            print("ERROR: Should have failed!")
        except OSError as e:
            print(f"✓ Correctly caught OSError for duplicate mkdir: {e}")
        
        # Test stat on non-existent path
        try:
            fsapi.stat('/nonexistent')  # This should fail
            print("ERROR: Should have failed!")
        except FileNotFoundError as e:
            print(f"✓ Correctly caught FileNotFoundError for stat: {e}")
        
        print("✓ Error condition testing completed")
        
    except Exception as e:
        print(f"Unexpected error during error testing: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        fs.close_filesystem()

if __name__ == "__main__":
    test_basic_operations()
    test_error_conditions()
