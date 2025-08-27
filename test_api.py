#!/usr/bin/env python3
"""
Test script for the filesystem API
"""

import os
from main import mkfs
import fsapi
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich import box

console = Console()

def test_basic_operations():
    """Test basic filesystem operations"""
    
    console.print(Panel.fit("[bold blue]🚀 Filesystem API Test Suite[/bold blue]", 
                           border_style="blue", padding=(1, 2)))
    
    # Create filesystem
    image_path = "test_fs.img"
    if os.path.exists(image_path):
        os.remove(image_path)
    
    console.print("\n[bold cyan]📁 Creating filesystem...[/bold cyan]")
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console
    ) as progress:
        task = progress.add_task("Initializing filesystem", total=None)
        mkfs(image_path)
        progress.update(task, completed=True)
    
    console.print("[green]✅ Filesystem created successfully![/green]")
    
    # Initialize filesystem API
    console.print("\n[bold cyan]🔌 Initializing filesystem API...[/bold cyan]")
    fs = fsapi.init_filesystem(image_path)
    console.print("[green]✅ Filesystem API initialized![/green]")
    
    try:
        # Test stat on root directory
        console.print("\n[bold magenta]📊 Testing stat on root directory[/bold magenta]")
        root_stat = fsapi.stat('/')
        
        # Create a nice table for stats
        stat_table = Table(title="Root Directory Statistics", box=box.ROUNDED)
        stat_table.add_column("Property", style="cyan", no_wrap=True)
        stat_table.add_column("Value", style="magenta")
        
        for key, value in root_stat.items():
            if key == 'type':
                value = f"[green]{value}[/green]"
            elif key == 'mode':
                value = f"[yellow]{oct(value)}[/yellow]"
            stat_table.add_row(key, str(value))
        
        console.print(stat_table)
        
        # Test readdir on root (should be empty initially)
        console.print("\n[bold magenta]📋 Testing readdir on empty root[/bold magenta]")
        root_files = fsapi.readdir('/')
        console.print(f"[dim]Root directory contents: {root_files if root_files else '[yellow]empty[/yellow]'}[/dim]")
        
        # Test creating a directory
        console.print("\n[bold magenta]📂 Testing mkdir[/bold magenta]")
        fsapi.mkdir('/testdir', 0o755)
        console.print("[green]✅ Created directory '/testdir'[/green]")
        
        # Check root directory now contains the new directory
        root_files = fsapi.readdir('/')
        console.print(f"[dim]Root directory contents: [green]{root_files}[/green][/dim]")
        
        # Test creating a file
        console.print("\n[bold magenta]📝 Testing file creation and writing[/bold magenta]")
        fd = fsapi.openf('/testfile.txt', fsapi.O_CREAT | fsapi.O_WRONLY)
        console.print(f"[green]✅ Created file, got fd: [bold]{fd}[/bold][/green]")
        
        # Write some data
        test_data = b"Hello, World! This is a test file.\nSecond line.\n"
        bytes_written = fsapi.write(fd, test_data)
        console.print(f"[green]✅ Wrote [bold]{bytes_written}[/bold] bytes[/green]")
        
        fsapi.close(fd)
        console.print("[green]✅ File closed[/green]")
        
        # Check directory contents after file creation
        root_files = fsapi.readdir('/')
        console.print(f"[dim]Root directory contents: [green]{root_files}[/green][/dim]")

        # Test reading the file
        console.print("\n[bold magenta]📖 Testing file reading[/bold magenta]")
        fd = fsapi.openf('/testfile.txt', fsapi.O_RDONLY)
        read_data = fsapi.read(fd, 1024)
        console.print(f"[green]✅ Read [bold]{len(read_data)}[/bold] bytes:[/green]")
        console.print(Panel(read_data.decode('utf-8', errors='replace'), 
                           title="[cyan]File Contents[/cyan]", border_style="green"))
        fsapi.close(fd)
        
        # Test file stats
        console.print("\n[bold magenta]📊 Testing file stat[/bold magenta]")
        file_stat = fsapi.stat('/testfile.txt')
        
        file_stat_table = Table(title="File Statistics", box=box.ROUNDED)
        file_stat_table.add_column("Property", style="cyan", no_wrap=True)
        file_stat_table.add_column("Value", style="magenta")
        
        for key, value in file_stat.items():
            if key == 'type':
                value = f"[green]{value}[/green]"
            elif key == 'mode':
                value = f"[yellow]{oct(value)}[/yellow]"
            elif key == 'size':
                value = f"[bold blue]{value}[/bold blue] bytes"
            file_stat_table.add_row(key, str(value))
        
        console.print(file_stat_table)
        
        # Test creating file in subdirectory
        console.print("\n[bold magenta]📁📝 Testing file creation in subdirectory[/bold magenta]")
        fd = fsapi.openf('/testdir/subfile.txt', fsapi.O_CREAT | fsapi.O_WRONLY)
        subfile_data = b"This is a file in subdirectory"
        fsapi.write(fd, subfile_data)
        fsapi.close(fd)
        console.print("[green]✅ Created file in subdirectory[/green]")
        
        # List subdirectory contents
        subdir_files = fsapi.readdir('/testdir')
        console.print(f"[dim]Subdirectory contents: [green]{subdir_files}[/green][/dim]")
        
        # Test truncating file
        console.print("\n[bold magenta]✂️ Testing file truncation[/bold magenta]")
        fd = fsapi.openf('/testfile.txt', fsapi.O_WRONLY | fsapi.O_TRUNC)
        new_data = b"Truncated and new content"
        fsapi.write(fd, new_data)
        fsapi.close(fd)
        console.print("[green]✅ File truncated and new content written[/green]")
        
        # Read truncated file
        fd = fsapi.openf('/testfile.txt', fsapi.O_RDONLY)
        truncated_data = fsapi.read(fd, 1024)
        console.print(f"[green]✅ After truncation:[/green]")
        console.print(Panel(truncated_data.decode('utf-8', errors='replace'), 
                           title="[cyan]Truncated File Contents[/cyan]", border_style="yellow"))
        fsapi.close(fd)
        
        console.print("\n[bold green]🎉 All tests completed successfully![/bold green]")
        
    except Exception as e:
        console.print(f"\n[bold red]❌ Error during testing: {e}[/bold red]")
        import traceback
        console.print(f"[red]{traceback.format_exc()}[/red]")
    
    finally:
        fs.close_filesystem()

def test_error_conditions():
    """Test error conditions"""
    
    console.print(Panel.fit("[bold red]🚨 Error Condition Testing[/bold red]", 
                           border_style="red", padding=(1, 2)))
    
    image_path = "test_fs.img"
    fs = fsapi.init_filesystem(image_path)
    
    try:
        tests_passed = 0
        total_tests = 4
        
        # Test opening non-existent file without O_CREAT
        try:
            fd = fsapi.openf('/nonexistent.txt', fsapi.O_RDONLY)
            console.print("[red]❌ ERROR: Should have failed![/red]")
        except FileNotFoundError as e:
            console.print(f"[green]✅ Correctly caught FileNotFoundError: [dim]{e}[/dim][/green]")
            tests_passed += 1
        
        # Test reading from write-only file
        fd = None
        try:
            fd = fsapi.openf('/testfile.txt', fsapi.O_WRONLY)
            fsapi.read(fd, 100)  # This should fail
            console.print("[red]❌ ERROR: Should have failed![/red]")
        except OSError as e:
            console.print(f"[green]✅ Correctly caught OSError: [dim]{e}[/dim][/green]")
            tests_passed += 1
        except FileNotFoundError as e:
            console.print(f"[green]✅ Correctly caught FileNotFoundError: [dim]{e}[/dim][/green]")
            tests_passed += 1
        finally:
            if fd is not None:
                fsapi.close(fd)
        
        # Test mkdir on existing directory
        try:
            fsapi.mkdir('/testdir', 0o755)
            console.print("[red]❌ ERROR: Should have failed![/red]")
        except OSError as e:
            console.print(f"[green]✅ Correctly caught OSError: [dim]{e}[/dim][/green]")
            tests_passed += 1
        
        # Test stat on non-existent path
        try:
            fsapi.stat('/nonexistent')  # This should fail
            console.print("[red]❌ ERROR: Should have failed![/red]")
        except FileNotFoundError as e:
            console.print(f"[green]✅ Correctly caught FileNotFoundError: [dim]{e}[/dim][/green]")
            tests_passed += 1
        
        # Summary
        success_rate = (tests_passed / total_tests) * 100
        if tests_passed == total_tests:
            console.print(f"\n[bold green]🎉 Error condition testing completed: {tests_passed}/{total_tests} tests passed ({success_rate:.0f}%)[/bold green]")
        else:
            console.print(f"\n[bold yellow]⚠️ Error condition testing completed: {tests_passed}/{total_tests} tests passed ({success_rate:.0f}%)[/bold yellow]")
        
    except Exception as e:
        console.print(f"\n[bold red]❌ Unexpected error during error testing: {e}[/bold red]")
        import traceback
        console.print(f"[red]{traceback.format_exc()}[/red]")
    
    finally:
        fs.close_filesystem()

if __name__ == "__main__":
    console.print(Panel.fit("[bold white on blue]🧪 EXT4-like Filesystem Test Suite 🧪[/bold white on blue]", 
                           padding=(1, 4)))
    
    test_basic_operations()
    test_error_conditions()
    
    console.print(Panel.fit("[bold green]✨ Test Suite Complete! ✨[/bold green]", 
                           border_style="green", padding=(1, 2)))
