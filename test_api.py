#!/usr/bin/env python3
"""
Test script for the filesystem API
"""

import os
from main import mkfs
import fsapi
from rich.console import Console

console = Console()


def test_basic_operations():
    """Test basic filesystem operations"""

    console.print("[bold blue]Filesystem API Test Suite[/bold blue]\n")

    image_path = "test_fs.img"
    if os.path.exists(image_path):
        os.remove(image_path)

    console.print("Creating filesystem...")
    mkfs(image_path)
    console.print("[green]Filesystem created[/green]")

    console.print("Initializing filesystem API...")
    fs = fsapi.init_filesystem(image_path)
    console.print("[green]API initialized[/green]")

    try:
        console.print("Testing stat on root directory")
        root_stat = fsapi.stat("/")

        console.print("Root directory stats:")
        for key, value in root_stat.items():
            if key == "mode":
                console.print(f"  {key}: {oct(value)}")
            else:
                console.print(f"  {key}: {value}")

        console.print("Testing readdir on empty root")
        root_files = fsapi.readdir("/")
        console.print(f"Root contents: {root_files if root_files else 'empty'}")

        console.print("Testing mkdir")
        fsapi.mkdir("/testdir", 0o755)
        console.print("[green]Created directory '/testdir'[/green]")

        root_files = fsapi.readdir("/")
        console.print(f"Root contents: {root_files}")

        console.print("Testing file creation and writing")
        fd = fsapi.openf("/testfile.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        console.print(f"Created file, fd: {fd}")

        test_data = b"Hello, World! This is a test file.\nSecond line.\n"
        bytes_written = fsapi.write(fd, test_data)
        console.print(f"Wrote {bytes_written} bytes")

        fsapi.close(fd)
        console.print("File closed")

        root_files = fsapi.readdir("/")
        console.print(f"Root contents: {root_files}")

        console.print("Testing file reading")
        fd = fsapi.openf("/testfile.txt", fsapi.O_RDONLY)
        read_data = fsapi.read(fd, 1024)
        console.print(f"Read {len(read_data)} bytes:")
        console.print(f"Content: {read_data.decode('utf-8', errors='replace')}")
        fsapi.close(fd)

        console.print("Testing file stat")
        file_stat = fsapi.stat("/testfile.txt")
        console.print("File stats:")
        for key, value in file_stat.items():
            if key == "mode":
                console.print(f"  {key}: {oct(value)}")
            elif key == "size":
                console.print(f"  {key}: {value} bytes")
            else:
                console.print(f"  {key}: {value}")

        console.print("Testing file creation in subdirectory")
        fd = fsapi.openf("/testdir/subfile.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        subfile_data = b"This is a file in subdirectory"
        fsapi.write(fd, subfile_data)
        fsapi.close(fd)
        console.print("[green]Created file in subdirectory[/green]")

        subdir_files = fsapi.readdir("/testdir")
        console.print(f"Subdirectory contents: {subdir_files}")

        console.print("Testing file truncation")
        fd = fsapi.openf("/testfile.txt", fsapi.O_WRONLY | fsapi.O_TRUNC)
        new_data = b"Truncated and new content"
        fsapi.write(fd, new_data)
        fsapi.close(fd)
        console.print("[green]File truncated and new content written[/green]")

        fd = fsapi.openf("/testfile.txt", fsapi.O_RDONLY)
        truncated_data = fsapi.read(fd, 1024)
        console.print("After truncation:")
        console.print(f"Content: {truncated_data.decode('utf-8', errors='replace')}")
        fsapi.close(fd)

        console.print("[bold green]All tests completed successfully[/bold green]")

    except Exception as e:
        console.print(f"[bold red]Error during testing: {e}[/bold red]")
        import traceback
        console.print(f"[red]{traceback.format_exc()}[/red]")

    finally:
        fs.close_filesystem()


def test_error_conditions():
    """Test error conditions"""

    console.print("[bold red]Error Condition Testing[/bold red]\n")

    image_path = "test_fs.img"
    fs = fsapi.init_filesystem(image_path)

    try:
        tests_passed = 0
        total_tests = 4

        try:
            fd = fsapi.openf("/nonexistent.txt", fsapi.O_RDONLY)
            console.print("[red]ERROR: Should have failed![/red]")
        except FileNotFoundError as e:
            console.print(f"[green]Correctly caught FileNotFoundError: {e}[/green]")
            tests_passed += 1

        fd = None
        try:
            fd = fsapi.openf("/testfile.txt", fsapi.O_WRONLY)
            fsapi.read(fd, 100)
            console.print("[red]ERROR: Should have failed![/red]")
        except OSError as e:
            console.print(f"[green]Correctly caught OSError: {e}[/green]")
            tests_passed += 1
        except FileNotFoundError as e:
            console.print(f"[green]Correctly caught FileNotFoundError: {e}[/green]")
            tests_passed += 1
        finally:
            if fd is not None:
                fsapi.close(fd)

        try:
            fsapi.mkdir("/testdir", 0o755)
            console.print("[red]ERROR: Should have failed![/red]")
        except OSError as e:
            console.print(f"[green]Correctly caught OSError: {e}[/green]")
            tests_passed += 1

        try:
            fsapi.stat("/nonexistent")
            console.print("[red]ERROR: Should have failed![/red]")
        except FileNotFoundError as e:
            console.print(f"[green]Correctly caught FileNotFoundError: {e}[/green]")
            tests_passed += 1

        success_rate = (tests_passed / total_tests) * 100
        if tests_passed == total_tests:
            console.print(f"[bold green]Error condition testing completed: {tests_passed}/{total_tests} tests passed ({success_rate:.0f}%)[/bold green]")
        else:
            console.print(f"[bold yellow]Error condition testing completed: {tests_passed}/{total_tests} tests passed ({success_rate:.0f}%)[/bold yellow]")

    except Exception as e:
        console.print(f"[bold red]Unexpected error during error testing: {e}[/bold red]")
        import traceback
        console.print(f"[red]{traceback.format_exc()}[/red]")

    finally:
        fs.close_filesystem()


if __name__ == "__main__":
    console.print("[bold white on blue]EXT4-like Filesystem Test Suite[/bold white on blue]\n")

    test_basic_operations()
    test_error_conditions()

    console.print("[bold green]Test Suite Complete[/bold green]")
