# import os
import posixpath
import sys
from fs import Extent
from fsapi import init_filesystem, get_filesystem, O_RDONLY, O_WRONLY, O_CREAT, O_TRUNC, S_IFMT, S_IFDIR, S_IFREG, S_IFLNK, S_IFIFO, S_IFCHR, S_IFBLK, S_IFSOCK, BLOCK_SIZE, Inode
from rich import print
import random
import string
commands = []

def command(name, description):
    def decorator(func):
        commands.append({'name': name, 'func': func, 'description': description})
        return func
    return decorator

def handle_help(args, cwd):
    print("Available commands:")
    for cmd in sorted(commands, key=lambda x: x['name']):
        print(f"  {cmd['name']}: {cmd['description']}")

@command('help', 'Show available commands')
def handle_help_decorated(args, cwd):
    handle_help(args, cwd)

def main():
    if len(sys.argv) > 1:
        image_path = sys.argv[1]
    else:
        image_path = "fs.img"

    try:
        init_filesystem(image_path)
        print(f"Filesystem loaded from {image_path}")
    except Exception as e:
        print(f"Error loading filesystem: {e}")
        return

    cwd = "/"

    while True:
        try:
            prompt = f"[bold cyan]{cwd}[/bold cyan][bold white]>[/bold white] "
            print(prompt, end="")
            cmd = input().strip()
            if not cmd:
                continue

            parts = cmd.split()
            command_name = parts[0].lower()
            args = parts[1:]

            if command_name == "exit" or command_name == "quit":
                break

            # Find command in list
            cmd_entry = next((c for c in commands if c['name'] == command_name), None)
            if cmd_entry:
                try:
                    result = cmd_entry['func'](args, cwd)
                    if result is not None:
                        cwd = result
                except Exception as e:
                    print(f"Error: {e}")
            else:
                print(f"Unknown command: {command_name}")

        except KeyboardInterrupt:
            print("\nExiting...")
            break
        except Exception as e:
            print(f"Error: {e}")

def resolve_path(path, cwd):
    if not path:
        return cwd
    if posixpath.isabs(path):
        path = posixpath.normpath(path)
    else:
        path = posixpath.normpath(posixpath.join(cwd, path))
    return path

@command('pwd', 'Print current working directory')
def handle_pwd(args, cwd):
    print(cwd)

@command('ls', 'List directory contents')
def handle_ls(args, cwd):
    fs = get_filesystem()
    if args:
        path = resolve_path(args[0], cwd)
    else:
        path = cwd
    try:
        entries = fs.readdir(path)
        # for entry in sorted(entries):
        #     print(entry)
        formatted = []
        for entry in sorted(entries):
            try:
                st = fs.stat(posixpath.join(path, entry))
                if st["type"] & S_IFDIR:
                    formatted.append(f"[bold blue]{entry}[/bold blue]")
                else:
                    formatted.append(entry)
            except FileNotFoundError:
                formatted.append(entry)  # Показываем имя, даже если файл «сломанный»

        print(*formatted, sep=" ")
    except Exception as e:
        print(f"ls: {e}")

@command('lsd', 'List directory contents with detailed information (-lAh)')
def handle_lsd(args, cwd):
    fs = get_filesystem()
    if args:
        path = resolve_path(args[0], cwd)
    else:
        path = cwd
    try:
        # Pre-resolve parent inode for performance
        parent_inode_num = fs._resolve_path(path)
        parent_inode = fs._get_inode(parent_inode_num)
        entries = fs.readdir(path)
        all_entries = sorted(entries)

        # Print table header
        print("Inode  Mode       Links  Uid  Gid    Size Name")
        print("-" * 60)

        for entry in all_entries:
            try:
                found_inode_num = fs._find_file_in_directory(parent_inode, entry)
                if found_inode_num is None:
                    continue

                inode = fs._get_inode(found_inode_num)

                # For stat info, try to follow symlinks but handle broken ones
                # Compute full path for stat
                entry_path = posixpath.join(path, entry)
                try:
                    stat_info = fs.stat(entry_path)
                    inode_num = found_inode_num
                except (FileNotFoundError, OSError):
                    # Broken symlink - use the symlink inode itself
                    stat_info = {"type": inode.mode & S_IFMT, "size": inode.size_lo}
                    inode_num = found_inode_num

                # File type and permissions
                mode = inode.mode
                # type_char = 'd' if stat_info["type"] & S_IFDIR else 'l' if stat_info["type"] & S_IFLNK else 'f' if stat_info["type"] & S_IFREG else 'c' if stat_info["type"] & S_IFCHR else 'b' if stat_info["type"] & S_IFBLK else 'p' if stat_info["type"] & S_IFIFO else 's' if stat_info["type"] & S_IFSOCK else '-'
                type_char = {
                    S_IFDIR: 'd',
                    S_IFLNK: 'l',
                    S_IFREG: '-',
                    S_IFCHR: 'c',
                    S_IFBLK: 'b',
                    S_IFIFO: 'p',
                    S_IFSOCK: 's'
                }.get(stat_info["type"], '?')

                # Convert to proper rwx format
                owner_perms = f"{'r' if mode & 0o400 else '-'}{'w' if mode & 0o200 else '-'}{'x' if mode & 0o100 else '-'}"
                group_perms = f"{'r' if mode & 0o040 else '-'}{'w' if mode & 0o020 else '-'}{'x' if mode & 0o010 else '-'}"
                other_perms = f"{'r' if mode & 0o004 else '-'}{'w' if mode & 0o002 else '-'}{'x' if mode & 0o001 else '-'}"

                # Human readable size
                size = stat_info["size"]
                if size < 1024:
                    size_str = f"{size}B"
                elif size < 1024 * 1024:
                    size_str = f"{size / 1024:.1f}K"
                elif size < 1024 * 1024 * 1024:
                    size_str = f"{size / (1024 * 1024):.1f}M"
                else:
                    size_str = f"{size / (1024 * 1024 * 1024):.1f}G"

                # Format entry name with color
                if stat_info["type"] & S_IFDIR:
                    entry_display = f"[bold blue]{entry}[/bold blue]"
                else:
                    entry_display = entry

                print(f"{inode_num:5d} {type_char}{owner_perms}{group_perms}{other_perms} {inode.links_count:3d} {inode.uid:4d} {inode.gid:4d} {size_str:>7s} {entry_display}")

            except Exception as e:
                print(f"lsd: error reading {entry}: {e}")

    except Exception as e:
        print(f"lsd: {e}")

@command('cd', 'Change current directory')
def handle_cd(args, cwd):
    fs = get_filesystem()
    if not args:
        new_path = "/"
    else:
        new_path = resolve_path(args[0], cwd)
    try:
        stat_info = fs.stat(new_path)
        if stat_info["type"] & S_IFDIR:
            return new_path
        else:
            print("cd: Not a directory")
            return cwd
    except Exception as e:
        print(f"cd: {e}")
        return cwd

@command('mkdir', 'Create a directory')
def handle_mkdir(args, cwd):
    fs = get_filesystem()
    if not args:
        print("mkdir: missing operand")
        return
    path = resolve_path(args[0], cwd)
    try:
        fs.mkdir(path)
    except Exception as e:
        print(f"mkdir: {e}")

@command('rmdir', 'Remove a directory')
def handle_rmdir(args, cwd):
    fs = get_filesystem()
    if not args:
        print("rmdir: missing operand")
        return
    path = resolve_path(args[0], cwd)
    try:
        fs.rmdir(path)
    except Exception as e:
        print(f"rmdir: {e}")

@command('rmdirr', 'Remove a directory and its contents')
def handle_rmdir_recursive(args, cwd):
    fs = get_filesystem()
    if not args:
        print("rmdir_recursive: missing operand")
        return
    path = resolve_path(args[0], cwd)
    try:
        fs.rmdir_recursive(path)
    except Exception as e:
        print(f"rmdir_recursive: {e}")

@command('rm', 'Remove a file')
def handle_rm(args, cwd):
    fs = get_filesystem()
    if not args:
        print("rm: missing operand")
        return
    path = resolve_path(args[0], cwd)
    try:
        fs.unlink(path)
    except Exception as e:
        print(f"rm: {e}")

@command('cat', 'Display file contents')
def handle_cat(args, cwd):
    fs = get_filesystem()
    if not args:
        print("cat: missing operand")
        return
    path = resolve_path(args[0], cwd)
    try:
        stat_info = fs.stat(path)
        if not (stat_info["type"] & (S_IFREG | S_IFLNK)):
            print("cat: Is not a regular file or symlink")
            return
        fd = fs.open(path, O_RDONLY)

        # Read up to 2000 bytes
        read_size = min(2000, stat_info["size"])
        data = fs.read(fd, read_size)
        content = data.decode('utf-8', errors='ignore')

        # Truncate if needed and add indication
        if stat_info["size"] > 2000:
            print(content)
            print(f"... [truncated, showing first 2000 bytes of {stat_info['size']} total]")
        else:
            print(content)

        fs.close(fd)
    except Exception as e:
        print(f"cat: {e}")

@command('touch', 'Create an empty file')
def handle_touch(args, cwd):
    fs = get_filesystem()
    if not args:
        print("touch: missing operand")
        return
    path = resolve_path(args[0], cwd)
    try:
        fd = fs.open(path, O_CREAT | O_WRONLY)
        fs.close(fd)
    except Exception as e:
        print(f"touch: {e}")

@command('cp', 'Copy files or directories')
def handle_cp(args, cwd):
    fs = get_filesystem()
    if len(args) < 2:
        print("cp: missing operand")
        return
    src_path = resolve_path(args[0], cwd)
    dst_path = resolve_path(args[1], cwd)
    try:
        # Check if source exists
        src_stat = fs.stat(src_path)
        if src_stat["type"] == "directory":
            print("cp: directories not supported yet")
            return

        # Read source file
        fd_src = fs.open(src_path, O_RDONLY)
        data = fs.read(fd_src, src_stat["size"])
        fs.close(fd_src)

        # Write to destination
        fd_dst = fs.open(dst_path, O_CREAT | O_WRONLY | O_TRUNC)
        fs.write(fd_dst, data)
        fs.close(fd_dst)
    except Exception as e:
        print(f"cp: {e}")

@command('mv', 'Move or rename files or directories')
def handle_mv(args, cwd):
    fs = get_filesystem()
    if len(args) < 2:
        print("mv: missing operand")
        return
    src_path = resolve_path(args[0], cwd)
    dst_raw = resolve_path(args[1], cwd)
    try:
        # Determine actual destination path: if dst_raw is directory, move inside it
        try:
            dst_stat = fs.stat(dst_raw)
            if dst_stat["type"] & S_IFDIR:
                dst_path = posixpath.join(dst_raw, posixpath.basename(src_path))
            else:
                dst_path = dst_raw
        except FileNotFoundError:
            dst_path = dst_raw
        # Copy + delete implementation
        # Check if source exists
        src_stat = fs.stat(src_path)
        if src_stat["type"] == "directory":
            print("mv: directories not supported yet")
            return

        # Read source file
        fd_src = fs.open(src_path, O_RDONLY)
        data = fs.read(fd_src, src_stat["size"])
        fs.close(fd_src)

        # Write to destination
        fd_dst = fs.open(dst_path, O_CREAT | O_WRONLY | O_TRUNC)
        fs.write(fd_dst, data)
        fs.close(fd_dst)

        # Remove source
        fs.unlink(src_path)
    except Exception as e:
        print(f"mv: {e}")

@command('echo', 'Display text')
def handle_echo(args, cwd):
    # Simple echo implementation with output redirection support
    if not args:
        print()
        return

    # Check for output redirection
    if '>' in args:
        redirect_idx = args.index('>')
        if redirect_idx == len(args) - 1:
            print("echo: missing filename for redirection")
            return

        # Text before '>' and filename after '>'
        text_args = args[:redirect_idx]
        filename = args[redirect_idx + 1]

        if len(args) > redirect_idx + 2:
            print("echo: too many arguments after redirection")
            return

        # Write to file
        fs = get_filesystem()
        file_path = resolve_path(filename, cwd)
        try:
            text = ' '.join(text_args) + '\n'
            # Truncate or create file without unlink
            fd = fs.open(file_path, O_CREAT | O_WRONLY | O_TRUNC)
            fs.write(fd, text.encode('utf-8'))
            fs.close(fd)
        except Exception as e:
            print(f"echo: {e}")
    else:
        # Normal echo to stdout
        print(' '.join(args))

@command('chmod', 'Change file permissions')
def handle_chmod(args, cwd):
    fs = get_filesystem()
    if len(args) < 2:
        print("chmod: missing operand")
        return
    mode_str = args[0]
    path = resolve_path(args[1], cwd)
    try:
        # Parse mode (simple implementation - assume octal)
        try:
            mode = int(mode_str, 8)
        except ValueError:
            print("chmod: invalid mode")
            return

        # Get current inode
        inode_num = fs._resolve_path(path)
        inode = fs._get_inode(inode_num)

        # Update mode (preserve file type bits)
        inode.mode = (inode.mode & 0o170000) | (mode & 0o0777)

        # Write back
        fs._write_inode(inode_num, inode)
    except Exception as e:
        print(f"chmod: {e}")

@command('chown', 'Change file owner')
def handle_chown(args, cwd):
    fs = get_filesystem()
    if len(args) < 2:
        print("chown: missing operand")
        return
    owner_str = args[0]
    path = resolve_path(args[1], cwd)
    try:
        # Parse owner (simple implementation - assume uid)
        try:
            uid = int(owner_str)
        except ValueError:
            print("chown: invalid owner")
            return

        # Get current inode
        inode_num = fs._resolve_path(path)
        inode = fs._get_inode(inode_num)

        # Update uid
        inode.uid = uid

        # Write back
        fs._write_inode(inode_num, inode)
    except Exception as e:
        print(f"chown: {e}")

@command('df', 'Display disk space usage')
def handle_df(args, cwd):
    fs = get_filesystem()
    try:
        sb = fs.superblock
        total_blocks = sb.fs_size_blocks
        free_blocks = sb.free_blocks_count
        used_blocks = total_blocks - free_blocks

        # Convert to MB (assuming 4KB blocks)
        block_size_mb = 4096 / (1024 * 1024)
        total_mb = total_blocks * block_size_mb
        used_mb = used_blocks * block_size_mb
        free_mb = free_blocks * block_size_mb

        print("Filesystem     1M-blocks  Used Available Use% Mounted on")
        print("rootfs              {:.0f}  {:.0f}      {:.0f}  {:.0f}% /".format(
            total_mb, used_mb, free_mb, (used_blocks / total_blocks) * 100))
    except Exception as e:
        print(f"df: {e}")

@command('du', 'Display directory space usage')
def handle_du(args, cwd):
    fs = get_filesystem()
    if args:
        path = resolve_path(args[0], cwd)
    else:
        path = cwd
    try:
        stat_info = fs.stat(path)
        size_kb = stat_info["size"] / 1024
        print("{:.0f}K\t{}".format(size_kb, path))
    except Exception as e:
        print(f"du: {e}")

@command('ln', 'Create links')
def handle_ln(args, cwd):
    fs = get_filesystem()
    if len(args) < 2:
        print("ln: missing operand")
        return

    target_path = resolve_path(args[0], cwd)
    link_path = resolve_path(args[1], cwd)

    try:
        # Prevent hard link to directory
        target_stat = fs.stat(target_path)
        if target_stat["type"] & S_IFDIR:
            print(f"ln: {target_path}: hard link not allowed for directory")
            return
        # Get target inode
        target_inode_num = fs._resolve_path(target_path)

        # Get parent directory of link
        link_parent = posixpath.dirname(link_path)
        link_name = posixpath.basename(link_path)

        if link_parent == "":
            link_parent = "/"

        parent_inode_num = fs._resolve_path(link_parent)

        # Add directory entry
        fs._add_directory_entry(parent_inode_num, link_name, target_inode_num, 1)

        # Increment link count
        target_inode = fs._get_inode(target_inode_num)
        target_inode.links_count += 1
        fs._write_inode(target_inode_num, target_inode)

    except Exception as e:
        print(f"ln: {e}")

@command('lns', 'Create a symbolic link')
def handle_lns(args, cwd):
    fs = get_filesystem()
    if len(args) < 2:
        print("lns: missing operand")
        return

    target_path = resolve_path(args[0], cwd)
    link_path = resolve_path(args[1], cwd)

    try:
        # Get parent directory of link
        link_parent = posixpath.dirname(link_path)
        link_name = posixpath.basename(link_path)

        if link_parent == "":
            link_parent = "/"

        parent_inode_num = fs._resolve_path(link_parent)

        # Allocate inode for symlink
        inode_num = fs._allocate_inode()

        # Allocate block for symlink target
        block = fs._allocate_block()

        # Create symlink inode
        target_path_bytes = target_path.encode('utf-8')
        size = len(target_path_bytes)
        inode = Inode(
            mode=S_IFLNK | 0o777,
            uid=0,
            size_lo=size,
            gid=0,
            links_count=1,
            size_high=0,
            atime=0,
            ctime=0,
            mtime=0,
            flags=0,
            extent_count=1,
            extents=[Extent(block, 1)] + [Extent(0, 0)] * 3,
        )

        fs._write_inode(inode_num, inode)

        # Write target path to the block
        fs.image_file.seek(block * BLOCK_SIZE)
        fs.image_file.write(target_path_bytes)
        fs.image_file.write(b"\x00" * (BLOCK_SIZE - size))
        fs.image_file.flush()

        # Add to parent directory
        fs._add_directory_entry(parent_inode_num, link_name, inode_num, 7)  # 7 for symlink

    except Exception as e:
        print(f"lns: {e}")

@command('rndfile', 'Create a file with random ASCII characters')
def handle_rndfile(args, cwd):
    fs = get_filesystem()
    if len(args) < 2:
        print("rndfile: missing operand (usage: rndfile filename size)")
        return

    filename = args[0]
    size_str = args[1]

    # Parse size string (e.g., "10M", "1K", "500B")
    try:
        if size_str.upper().endswith('B'):
            size = int(size_str[:-1])
        elif size_str.upper().endswith('K'):
            size = int(size_str[:-1]) * 1024
        elif size_str.upper().endswith('M'):
            size = int(size_str[:-1]) * 1024 * 1024
            if size > 150 * 1024 * 1024:
                print("rndfile: too large")
                return
        elif size_str.upper().endswith('G'):
            print("rndfile: too large")
            return
            # size = int(size_str[:-1]) * 1024 * 1024 * 1024
        else:
            size = int(size_str)
    except ValueError:
        print("rndfile: invalid size format")
        return

    if size <= 0:
        print("rndfile: size must be positive")
        return

    file_path = resolve_path(filename, cwd)

    try:
        # Remove existing file to ensure new inode
        try:
            fs.unlink(file_path)
        except FileNotFoundError:
            pass
        # Create file
        fd = fs.open(file_path, O_CREAT | O_WRONLY | O_TRUNC)

        # Write random ASCII characters in chunks to avoid memory issues
        chunk_size = min(1024 * 1024, size)  # 1MB chunks or smaller
        written = 0

        while written < size:
            remaining = size - written
            current_chunk = min(chunk_size, remaining)

            # Generate random ASCII printable characters
            random_chars = ''.join(random.choices(
                string.ascii_letters + string.digits + string.punctuation + ' ',
                k=current_chunk
            ))
            fs.write(fd, random_chars.encode('utf-8'))
            written += current_chunk

        fs.close(fd)
        print(f"Created {filename} with {size} bytes of random ASCII data")

    except Exception as e:
        print(f"rndfile: {e}")

@command('stat', 'Display file or directory status')
def handle_stat(args, cwd):
    fs = get_filesystem()
    if not args:
        print("stat: missing operand")
        return
    path = resolve_path(args[0], cwd)
    try:
        stat_info = fs.stat(path)
        inode_num = fs._resolve_path(path)
        inode = fs._get_inode(inode_num)

        type_names = {
            S_IFDIR: "directory",
            S_IFLNK: "symbolic link",
            S_IFREG: "regular file",
            S_IFIFO: "fifo",
            S_IFCHR: "character device",
            S_IFBLK: "block device",
            S_IFSOCK: "socket",
        }
        print(f"  File: {path}")
        print(f"  Size: {stat_info['size']}\t\tBlocks: {(stat_info['size'] + 4095) // 4096}")
        print(f"  Type: {type_names.get(stat_info['type'], 'unknown')}")
        print(f"Inode: {inode_num}\t\tLinks: {inode.links_count}")
        print(f"Access: ({inode.mode & 0o777:04o})\t\tUid: {inode.uid}\t\tGid: {inode.gid}")
        print(f"Access: {inode.atime}")
        print(f"Modify: {inode.mtime}")
        print(f"Change: {inode.ctime}")
    except Exception as e:
        print(f"stat: {e}")

if __name__ == "__main__":
    main()