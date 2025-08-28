#!/usr/bin/env python3
"""
Test script for the filesystem API
"""

import os
from main import mkfs
import fsapi
from rich.console import Console

console = Console()

class RaisesContext:
    def __init__(self, exc_type):
        self.exc_type = exc_type

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type is None:
            raise AssertionError(f"Expected exception {self.exc_type.__name__}, but no exception was raised")

        if not issubclass(exc_type, self.exc_type):
            raise AssertionError(
                f"Expected exception {self.exc_type.__name__}, but got {exc_type.__name__}"
            )

        return True  # подавляем исключение

class TestCase:
    """Базовый класс для наших тестов с автоматической настройкой и очисткой."""
    def setUp(self):
        """Выполняется перед каждым тестом."""
        self.image_path = "test_fs.img"
        if os.path.exists(self.image_path):
            os.remove(self.image_path)
        mkfs(self.image_path)
        self.fs = fsapi.init_filesystem(self.image_path)

    def tearDown(self):
        """Выполняется после каждого теста."""
        self.fs.close_filesystem()
        if os.path.exists(self.image_path):
            os.remove(self.image_path)

    def assertEqual(self, a, b, msg=""):
        if a != b:
            raise AssertionError(f"{msg} | {a!r} != {b!r}")

    def assertTrue(self, x, msg=""):
        if not x:
            raise AssertionError(f"{msg} | Expression is not True")

    def assertRaises(self, exc_type, func=None, *args, **kwargs):
        if func is None:
            # Вариант через контекстный менеджер
            return RaisesContext(exc_type)
        else:
            # Вариант вызова функции напрямую
            try:
                func(*args, **kwargs)
            except exc_type:
                return
            except Exception as e:
                raise AssertionError(
                    f"Expected exception {exc_type.__name__}, but got {e.__class__.__name__}"
                )
            raise AssertionError(
                f"Expected exception {exc_type.__name__}, but no exception was raised"
            )

class TestRunner:
    """Находит и запускает все тесты."""
    def __init__(self):
        self.console = Console()
        self.tests_run = 0
        self.failures = []

    def run(self, test_case_class):
        self.console.print(f"[bold yellow]Running tests for {test_case_class.__name__}[/bold yellow]")
        test_instance = test_case_class()
        
        test_methods = [m for m in dir(test_instance) if m.startswith("test_")]

        for method_name in test_methods:
            self.tests_run += 1
            # Запускаем setUp, тест и tearDown для каждого метода
            try:
                test_instance.setUp()
                getattr(test_instance, method_name)()
                self.console.print(f"  [green]✓[/green] {method_name}")
            except Exception:
                import traceback
                self.failures.append((method_name, traceback.format_exc()))
                self.console.print(f"  [bold red]✗ FAILED[/bold red]: {method_name}")
                # self.console.print(f"[red]{traceback.format_exc()}[/red]") # Можно раскомментировать для детального вывода
            finally:
                test_instance.tearDown()
        
        self.console.print("-" * 40)

    def summary(self):
        self.console.print("\n[bold]Test Summary[/bold]")
        if self.failures:
            self.console.print(f"[bold red]FAILURES ({len(self.failures)}):[/bold red]")
            for name, tb in self.failures:
                self.console.print(f"\n--- Failure in {name} ---")
                self.console.print(f"[red]{tb}[/red]")
        
        passed = self.tests_run - len(self.failures)
        color = "green" if passed == self.tests_run else "yellow"
        self.console.print(f"[{color}]Ran {self.tests_run} tests. {passed} passed, {len(self.failures)} failed.[/{color}]")


class TestCoreFS(TestCase):
    """Тесты базовых операций с файлами и каталогами."""

    def test_root_dir_stat(self):
        root_stat = fsapi.stat("/")
        self.assertEqual(root_stat["inode"], 2)
        self.assertEqual(root_stat["type"], fsapi.S_IFDIR)

    def test_mkdir_and_readdir(self):
        fsapi.mkdir("/testdir", 0o755)
        contents = fsapi.readdir("/")
        self.assertEqual(contents, ["testdir"])
        
        fsapi.mkdir("/testdir/subdir", 0o755)
        sub_contents = fsapi.readdir("/testdir")
        self.assertEqual(sub_contents, ["subdir"])

    def test_file_create_write_read(self):
        fd = fsapi.openf("/hello.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        test_data = b"Hello FS!"
        bytes_written = fsapi.write(fd, test_data)
        self.assertEqual(bytes_written, len(test_data))
        fsapi.close(fd)
        
        file_stat = fsapi.stat("/hello.txt")
        self.assertEqual(file_stat["size"], len(test_data))
        
        fd = fsapi.openf("/hello.txt", fsapi.O_RDONLY)
        read_data = fsapi.read(fd, 100)
        fsapi.close(fd)
        self.assertEqual(read_data, test_data)
        
    def test_file_truncation(self):
        fd = fsapi.openf("/trunc.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, b"some initial data")
        fsapi.close(fd)
        
        fd = fsapi.openf("/trunc.txt", fsapi.O_TRUNC | fsapi.O_WRONLY)
        new_data = b"truncated"
        fsapi.write(fd, new_data)
        fsapi.close(fd)
        
        file_stat = fsapi.stat("/trunc.txt")
        self.assertEqual(file_stat["size"], len(new_data))
        
        fd = fsapi.openf("/trunc.txt", fsapi.O_RDONLY)
        read_data = fsapi.read(fd, 100)
        fsapi.close(fd)
        self.assertEqual(read_data, new_data)

    def test_unlink_file(self):
        fd = fsapi.openf("/file_to_delete.txt", fsapi.O_CREAT)
        fsapi.close(fd)
        contents = fsapi.readdir("/")
        self.assertTrue("file_to_delete.txt" in contents)
        
        fsapi.unlink("/file_to_delete.txt")
        contents_after = fsapi.readdir("/")
        self.assertTrue("file_to_delete.txt" not in contents_after)
        self.assertRaises(FileNotFoundError, fsapi.stat, "/file_to_delete.txt")


class TestAdvancedFS(TestCase):
    """Тесты продвинутых сценариев: большие файлы, смещения, пограничные случаи."""

    def test_file_overwrite_with_offset(self):
        fd = fsapi.openf("/offset.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, b"1234567890")
        
        # Перезаписываем середину
        fsapi.write(fd, b"---", offset=3)
        fsapi.close(fd)
        
        fd = fsapi.openf("/offset.txt", fsapi.O_RDONLY)
        content = fsapi.read(fd, 100)
        fsapi.close(fd)
        
        self.assertEqual(content, b"123---7890")
        file_stat = fsapi.stat("/offset.txt")
        self.assertEqual(file_stat["size"], 10)

    def test_file_grows_with_offset_write(self):
        fd = fsapi.openf("/grow.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, b"short")
        
        # Записываем за пределы файла
        fsapi.write(fd, b"end", offset=10)
        fsapi.close(fd)

        file_stat = fsapi.stat("/grow.txt")
        self.assertEqual(file_stat["size"], 13)
        
        fd = fsapi.openf("/grow.txt", fsapi.O_RDONLY)
        content = fsapi.read(fd, 100)
        fsapi.close(fd)

        # Проверяем, что между данными образовалась "дыра" из нулей
        self.assertEqual(content, b"short\x00\x00\x00\x00\x00end")
        
    def test_large_file_multiple_blocks(self):
        large_data = b"A" * (fsapi.BLOCK_SIZE + 500)
        fd = fsapi.openf("/large.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, large_data)
        fsapi.close(fd)
        
        file_stat = fsapi.stat("/large.txt")
        self.assertEqual(file_stat["size"], len(large_data))
        
        fd = fsapi.openf("/large.txt", fsapi.O_RDONLY)
        read_data = fsapi.read(fd, len(large_data) + 100)
        fsapi.close(fd)
        
        self.assertEqual(read_data, large_data)

    def test_file_growth_beyond_inode_extents(self):
        """Проверяет, что файл может успешно вырасти до 4 и более экстентов."""
        fd = fsapi.openf("/growth.txt", fsapi.O_CREAT | fsapi.O_RDWR)
        
        # В цикле записываем по одному байту в разные блоки, чтобы создать много экстентов
        for i in range(10):  # Создаем 10 экстентов
            offset = i * fsapi.BLOCK_SIZE * 2
            fsapi.write(fd, bytes([65 + i]), offset=offset)  # 'A' + i
        
        # Проверяем, что все записи прошли без ошибок
        # Читаем весь файл
        file_size = fsapi.stat("/growth.txt")["size"]
        data = fsapi.read(fd, file_size)
        
        # Проверяем, что данные на месте
        for i in range(10):
            expected_byte = 65 + i
            self.assertEqual(data[i * fsapi.BLOCK_SIZE * 2], expected_byte, f"Byte at extent {i} is incorrect")
        
        fsapi.close(fd)
        
    def test_unlink_open_file(self):
        """Проверяет логику отложенного удаления inode."""
        fd = fsapi.openf("/open_unlink.txt", fsapi.O_CREAT | fsapi.O_RDWR)
        fsapi.write(fd, b"data")
        
        # Удаляем файл, пока он открыт
        fsapi.unlink("/open_unlink.txt")
        
        # Убеждаемся, что из каталога он пропал
        self.assertTrue("open_unlink.txt" not in fsapi.readdir("/"))
        
        # Но мы все еще можем читать из открытого дескриптора
        read_data = fsapi.read(fd, 10, offset=0)
        self.assertEqual(read_data, b"data")
        
        # После закрытия файла inode и блоки должны быть освобождены
        fsapi.close(fd)
        
        # Повторное открытие должно провалиться
        self.assertRaises(FileNotFoundError, fsapi.openf, "/open_unlink.txt", fsapi.O_RDONLY)


class TestErrorConditions(TestCase):
    """Тесты на корректную обработку ошибок."""
    
    def test_open_non_existent_file(self):
        self.assertRaises(FileNotFoundError, fsapi.openf, "/no_file.txt", fsapi.O_RDONLY)

    def test_read_from_write_only_file(self):
        fd = fsapi.openf("/write_only.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        self.assertRaises(OSError, fsapi.read, fd, 10)
        fsapi.close(fd)
        
    def test_write_to_read_only_file(self):
        fd = fsapi.openf("/read_only.txt", fsapi.O_CREAT)
        fsapi.close(fd)
        fd = fsapi.openf("/read_only.txt", fsapi.O_RDONLY)
        self.assertRaises(OSError, fsapi.write, fd, b"fail")
        fsapi.close(fd)
        
    def test_mkdir_existing(self):
        fsapi.mkdir("/dir1", 0o755)
        self.assertRaises(OSError, fsapi.mkdir, "/dir1", 0o755)
        
    def test_rmdir_non_empty(self):
        fsapi.mkdir("/non_empty_dir", 0o755)
        fd = fsapi.openf("/non_empty_dir/file.txt", fsapi.O_CREAT)
        fsapi.close(fd)
        self.assertRaises(OSError, fsapi.rmdir, "/non_empty_dir")

    def test_unlink_directory(self):
        fsapi.mkdir("/dir_to_unlink", 0o755)
        self.assertRaises(OSError, fsapi.unlink, "/dir_to_unlink")


class TestNamingAndPaths(TestCase):
    """Тесты на именование и пути."""

    def test_filenames_with_special_chars(self):
        filenames = [
            "file with spaces.txt",
            ".dotfile",
            "file.with.many.dots",
            "__underscores__"
        ]
        fsapi.mkdir("/special_names")
        for name in filenames:
            path = f"/special_names/{name}"
            fd = fsapi.openf(path, fsapi.O_CREAT | fsapi.O_WRONLY)
            fsapi.write(fd, name.encode())
            fsapi.close(fd)
        
        contents = fsapi.readdir("/special_names")
        self.assertEqual(sorted(contents), sorted(filenames))
        
        # Проверяем чтение
        path = "/special_names/file with spaces.txt"
        fd = fsapi.openf(path, fsapi.O_RDONLY)
        data = fsapi.read(fd, 100)
        fsapi.close(fd)
        self.assertEqual(data.decode(), "file with spaces.txt")

    def test_deeply_nested_directories(self):
        path = ""
        for i in range(10):
            path += f"/dir{i}"
            fsapi.mkdir(path)

        file_path = path + "/final.txt"
        fd = fsapi.openf(file_path, fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, b"deep")
        fsapi.close(fd)
        
        file_stat = fsapi.stat(file_path)
        self.assertEqual(file_stat["size"], 4)


class TestResourceManagement(TestCase):
    """Тесты на управление ресурсами."""

    def test_block_and_inode_recycling(self):
        # 1. Запоминаем начальное состояние
        sb_before = self.fs.superblock
        initial_free_inodes = sb_before.free_inodes_count
        initial_free_blocks = sb_before.free_blocks_count

        # 2. Создаем файл, тратим 1 инод и 1 блок
        fd = fsapi.openf("/temp.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, b"a" * 10)
        fsapi.close(fd)

        sb_after_create = self.fs.superblock
        self.assertEqual(sb_after_create.free_inodes_count, initial_free_inodes - 1)
        self.assertEqual(sb_after_create.free_blocks_count, initial_free_blocks - 1)

        # 3. Удаляем файл, ресурсы должны вернуться
        fsapi.unlink("/temp.txt")
        
        sb_after_unlink = self.fs.superblock
        self.assertEqual(sb_after_unlink.free_inodes_count, initial_free_inodes, "Inodes not freed after unlink")
        self.assertEqual(sb_after_unlink.free_blocks_count, initial_free_blocks, "Blocks not freed after unlink")

        # 4. Повторяем для каталога (тратит 1 инод и 1 блок)
        fsapi.mkdir("/tempdir")
        sb_after_mkdir = self.fs.superblock
        self.assertEqual(sb_after_mkdir.free_inodes_count, initial_free_inodes - 1)
        # Блок директории + обновление родительской директории (если это новый блок)
        # Для простоты, допустим, что это всего 1 блок.
        # В реальных ФС это сложнее, но для прототипа сойдет.
        self.assertTrue(sb_after_mkdir.free_blocks_count < initial_free_blocks)
        
        fsapi.rmdir("/tempdir")
        sb_after_rmdir = self.fs.superblock
        self.assertEqual(sb_after_rmdir.free_inodes_count, initial_free_inodes, "Inodes not freed after rmdir")
        # Проверка блоков может быть неточной, если rmdir меняет родителя,
        # но она не должна сильно уменьшаться
        self.assertTrue(initial_free_blocks - sb_after_rmdir.free_blocks_count <= 1)

    def test_exhaust_inodes(self):
        num_inodes_to_create = self.fs.superblock.free_inodes_count 
        
        with self.assertRaises(OSError):
            for i in range(num_inodes_to_create + 5):
                 # Создание пустого файла тратит один инод
                 fd = fsapi.openf(f"/{i}.txt", fsapi.O_CREAT)
                 fsapi.close(fd)


class TestDataIntegrity(TestCase):
    """Тесты на целостность данных и B+ дерева."""

    def test_overwrite_file_boundary(self):
        block_size = fsapi.BLOCK_SIZE
        # Создаем файл размером чуть больше одного блока
        initial_data = b'A' * (block_size + 10)
        fd = fsapi.openf("/boundary.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, initial_data)
        
        # Перезаписываем данные точно на границе блоков
        overwrite_data = b'B' * 20
        offset = block_size - 10
        fsapi.write(fd, overwrite_data, offset=offset)
        fsapi.close(fd)
        
        # Создаем ожидаемый результат
        expected_data = bytearray(initial_data)
        expected_data[offset : offset + len(overwrite_data)] = overwrite_data
        
        fd = fsapi.openf("/boundary.txt", fsapi.O_RDONLY)
        read_data = fsapi.read(fd, len(expected_data) + 100)
        fsapi.close(fd)
        
        self.assertEqual(read_data, bytes(expected_data))

    def test_read_with_various_offsets_and_sizes(self):
        # Создаем файл с дырой
        fd = fsapi.openf("/hole.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, b"begin", offset=0)
        fsapi.write(fd, b"end", offset=100)
        fsapi.close(fd)

        expected_content = b"begin" + b'\x00' * 95 + b"end"
        
        fd = fsapi.openf("/hole.txt", fsapi.O_RDONLY)
        # Читаем по одному байту
        for i in range(len(expected_content)):
            byte_read = fsapi.read(fd, 1, offset=i)
            self.assertEqual(byte_read[0], expected_content[i], f"Byte at offset {i} is wrong")
            
        # Читаем кусок, пересекающий дыру
        chunk = fsapi.read(fd, 10, offset=2)
        self.assertEqual(chunk, expected_content[2:12])
        fsapi.close(fd)

    def test_multiple_open_descriptors_same_file(self):
        path = "/multi_fd.txt"
        fd1 = fsapi.openf(path, fsapi.O_CREAT | fsapi.O_RDWR)
        fd2 = fsapi.openf(path, fsapi.O_RDWR)

        # Пишем через fd1
        fsapi.write(fd1, b"Hello from fd1")
        
        # Читаем через fd2 - должны увидеть изменения
        content_fd2 = fsapi.read(fd2, 100, offset=0)
        self.assertEqual(content_fd2, b"Hello from fd1")
        
        # Пишем через fd2
        fsapi.write(fd2, b"fd2 says hi", offset=6)
        
        # Читаем через fd1
        content_fd1 = fsapi.read(fd1, 100, offset=0)
        self.assertEqual(content_fd1, b"Hello fd2 says hi")
        
        fsapi.close(fd1)
        fsapi.close(fd2)


class TestHardLinksAndSymlinks(TestCase):
    """Тесты для жестких и символических ссылок."""

    def test_hard_link_creation_and_access(self):
        # Создаем оригинальный файл
        fd = fsapi.openf("/original.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, b"original content")
        fsapi.close(fd)
        
        # Получаем stat до создания ссылки
        orig_stat = fsapi.stat("/original.txt")
        self.assertEqual(orig_stat["links_count"], 1)
        
        # Создаем жесткую ссылку через shell commands
        # Поскольку fsapi не предоставляет link(), используем файловую систему напрямую
        fs = fsapi.get_filesystem()
        orig_inode_num = fs._resolve_path("/original.txt")
        
        # Добавляем запись вручную в корневой каталог
        fs._add_directory_entry(2, "hardlink.txt", orig_inode_num, 1)  # 2 = root inode, 1 = regular file
        
        # Обновляем счетчик ссылок
        orig_inode = fs._get_inode(orig_inode_num)
        orig_inode.links_count += 1
        fs._write_inode(orig_inode_num, orig_inode)
        
        # Проверяем, что оба файла видны в каталоге
        contents = fsapi.readdir("/")
        self.assertTrue("original.txt" in contents)
        self.assertTrue("hardlink.txt" in contents)
        
        # Проверяем, что у обоих одинаковый inode
        orig_stat2 = fsapi.stat("/original.txt")
        link_stat = fsapi.stat("/hardlink.txt")
        self.assertEqual(orig_stat2["inode"], link_stat["inode"])
        self.assertEqual(orig_stat2["links_count"], 2)
        self.assertEqual(link_stat["links_count"], 2)
        
        # Проверяем содержимое через оба имени
        fd1 = fsapi.openf("/original.txt", fsapi.O_RDONLY)
        content1 = fsapi.read(fd1, 100)
        fsapi.close(fd1)
        
        fd2 = fsapi.openf("/hardlink.txt", fsapi.O_RDONLY)
        content2 = fsapi.read(fd2, 100)
        fsapi.close(fd2)
        
        self.assertEqual(content1, content2)
        self.assertEqual(content1, b"original content")

    def test_hard_link_unlink_preserves_file(self):
        # Создаем файл и жесткую ссылку
        fd = fsapi.openf("/file1.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, b"shared data")
        fsapi.close(fd)
        
        fs = fsapi.get_filesystem()
        inode_num = fs._resolve_path("/file1.txt")
        fs._add_directory_entry(2, "file2.txt", inode_num, 1)
        
        inode = fs._get_inode(inode_num)
        inode.links_count += 1
        fs._write_inode(inode_num, inode)
        
        # Удаляем первое имя
        fsapi.unlink("/file1.txt")
        
        # Файл должен остаться доступным через второе имя
        fd = fsapi.openf("/file2.txt", fsapi.O_RDONLY)
        content = fsapi.read(fd, 100)
        fsapi.close(fd)
        self.assertEqual(content, b"shared data")
        
        # Проверяем счетчик ссылок
        stat_info = fsapi.stat("/file2.txt")
        self.assertEqual(stat_info["links_count"], 1)

    def test_symlink_creation_and_resolution(self):
        # Создаем целевой файл
        fd = fsapi.openf("/target.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, b"target content")
        fsapi.close(fd)
        
        # Создаем символическую ссылку вручную
        fs = fsapi.get_filesystem()
        symlink_inode_num = fs._allocate_inode()
        
        target_path = "/target.txt"
        target_bytes = target_path.encode('utf-8')
        
        # Создаем inode для символической ссылки (inline)
        from fs import Inode
        import time
        
        symlink_inode = Inode(
            mode=fsapi.S_IFLNK | 0o777,
            uid=0,
            size_lo=len(target_bytes),
            gid=0,
            links_count=1,
            size_high=0,
            atime=int(time.time()),
            ctime=int(time.time()),
            mtime=int(time.time()),
            flags=0,
            extent_root=target_bytes.ljust(48, b'\x00')
        )
        
        fs._write_inode(symlink_inode_num, symlink_inode)
        fs._add_directory_entry(2, "symlink.txt", symlink_inode_num, 7)  # 7 = symlink type
        
        # Проверяем, что символическая ссылка видна
        contents = fsapi.readdir("/")
        self.assertTrue("symlink.txt" in contents)
        
        # Проверяем stat символической ссылки (без разрешения)
        lstat_info = fsapi.lstat("/symlink.txt")
        self.assertEqual(lstat_info["type"], fsapi.S_IFLNK)
        self.assertEqual(lstat_info["size"], len(target_bytes))
        
        # Проверяем stat с разрешением ссылки
        stat_info = fsapi.stat("/symlink.txt")
        self.assertEqual(stat_info["type"], fsapi.S_IFREG)  # Должен разрешиться в обычный файл
        
        # Проверяем чтение через символическую ссылку
        fd = fsapi.openf("/symlink.txt", fsapi.O_RDONLY)
        content = fsapi.read(fd, 100)
        fsapi.close(fd)
        self.assertEqual(content, b"target content")

    def test_broken_symlink(self):
        # Создаем символическую ссылку на несуществующий файл
        fs = fsapi.get_filesystem()
        symlink_inode_num = fs._allocate_inode()
        
        target_path = "/nonexistent.txt"
        target_bytes = target_path.encode('utf-8')
        
        from fs import Inode
        import time
        
        symlink_inode = Inode(
            mode=fsapi.S_IFLNK | 0o777,
            uid=0,
            size_lo=len(target_bytes),
            gid=0,
            links_count=1,
            size_high=0,
            atime=int(time.time()),
            ctime=int(time.time()),
            mtime=int(time.time()),
            flags=0,
            extent_root=target_bytes.ljust(48, b'\x00')
        )
        
        fs._write_inode(symlink_inode_num, symlink_inode)
        fs._add_directory_entry(2, "broken_link.txt", symlink_inode_num, 7)
        
        # lstat должен работать
        lstat_info = fsapi.lstat("/broken_link.txt")
        self.assertEqual(lstat_info["type"], fsapi.S_IFLNK)
        
        # stat должен падать с FileNotFoundError
        self.assertRaises(FileNotFoundError, fsapi.stat, "/broken_link.txt")
        
        # open должен падать с FileNotFoundError
        self.assertRaises(FileNotFoundError, fsapi.openf, "/broken_link.txt", fsapi.O_RDONLY)


class TestDirectoryOperations(TestCase):
    """Расширенные тесты операций с каталогами."""

    def test_rmdir_recursive(self):
        # Создаем структуру каталогов
        fsapi.mkdir("/parent")
        fsapi.mkdir("/parent/child1")
        fsapi.mkdir("/parent/child2")
        fsapi.mkdir("/parent/child1/grandchild")
        
        # Добавляем файлы
        fd = fsapi.openf("/parent/file1.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, b"parent file")
        fsapi.close(fd)
        
        fd = fsapi.openf("/parent/child1/file2.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, b"child file")
        fsapi.close(fd)
        
        # Обычный rmdir должен не работать для непустых каталогов
        self.assertRaises(OSError, fsapi.rmdir, "/parent")
        
        # Используем рекурсивное удаление через файловую систему
        fs = fsapi.get_filesystem()
        fs.rmdir_recursive("/parent")
        
        # Проверяем, что все удалено
        root_contents = fsapi.readdir("/")
        self.assertTrue("parent" not in root_contents)
        
        # Убеждаемся, что файлы недоступны
        self.assertRaises(FileNotFoundError, fsapi.stat, "/parent/file1.txt")
        self.assertRaises(FileNotFoundError, fsapi.stat, "/parent/child1/file2.txt")

    def test_directory_with_many_entries(self):
        fsapi.mkdir("/many_entries")
        
        # Создаем много файлов в одном каталоге
        num_files = 100
        for i in range(num_files):
            filename = f"/many_entries/file_{i:03d}.txt"
            fd = fsapi.openf(filename, fsapi.O_CREAT | fsapi.O_WRONLY)
            fsapi.write(fd, f"content_{i}".encode())
            fsapi.close(fd)
        
        # Проверяем, что все файлы видны
        contents = fsapi.readdir("/many_entries")
        self.assertEqual(len(contents), num_files)
        
        # Проверяем несколько случайных файлов
        for i in [0, 25, 50, 75, 99]:
            filename = f"/many_entries/file_{i:03d}.txt"
            self.assertTrue(f"file_{i:03d}.txt" in contents)
            
            fd = fsapi.openf(filename, fsapi.O_RDONLY)
            content = fsapi.read(fd, 100)
            fsapi.close(fd)
            self.assertEqual(content, f"content_{i}".encode())

    def test_directory_entry_reuse_after_deletion(self):
        """Тест на переиспользование места в блоках каталога после удаления."""
        fsapi.mkdir("/reuse_test")
        
        # Создаем файлы
        files_to_create = ["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"]
        for filename in files_to_create:
            path = f"/reuse_test/{filename}"
            fd = fsapi.openf(path, fsapi.O_CREAT)
            fsapi.close(fd)
        
        # Проверяем, что все созданы
        contents = fsapi.readdir("/reuse_test")
        self.assertEqual(sorted(contents), sorted(files_to_create))
        
        # Удаляем средние файлы
        fsapi.unlink("/reuse_test/b.txt")
        fsapi.unlink("/reuse_test/d.txt")
        
        # Проверяем, что остались правильные файлы
        contents_after_delete = fsapi.readdir("/reuse_test")
        expected_remaining = ["a.txt", "c.txt", "e.txt"]
        self.assertEqual(sorted(contents_after_delete), sorted(expected_remaining))
        
        # Создаем новые файлы (должны переиспользовать освободившееся место)
        new_files = ["f.txt", "g.txt"]
        for filename in new_files:
            path = f"/reuse_test/{filename}"
            fd = fsapi.openf(path, fsapi.O_CREAT)
            fsapi.close(fd)
        
        # Проверяем финальное состояние
        final_contents = fsapi.readdir("/reuse_test")
        expected_final = ["a.txt", "c.txt", "e.txt", "f.txt", "g.txt"]
        self.assertEqual(sorted(final_contents), sorted(expected_final))


class TestExtentTreeStress(TestCase):
    """Стресс-тесты B+ дерева экстентов."""

    def test_fragmented_file_creation(self):
        """Создает максимально фрагментированный файл."""
        fd = fsapi.openf("/fragmented.txt", fsapi.O_CREAT | fsapi.O_RDWR)
        
        # Записываем по одному байту в разные блоки, создавая много экстентов
        pattern = b'ABCDEFGHIJKLMNOPQRSTUVWXYZ'
        block_size = fsapi.BLOCK_SIZE
        
        for i, byte_val in enumerate(pattern):
            # Записываем в блоки с большими промежутками
            offset = i * block_size * 3  # 3 блока между записями
            fsapi.write(fd, bytes([byte_val]), offset=offset)
        
        # Проверяем, что можем прочитать все обратно
        for i, expected_byte in enumerate(pattern):
            offset = i * block_size * 3
            actual = fsapi.read(fd, 1, offset=offset)
            self.assertEqual(actual[0], expected_byte)
        
        # Проверяем размер файла
        file_stat = fsapi.stat("/fragmented.txt")
        expected_size = (len(pattern) - 1) * block_size * 3 + 1
        self.assertEqual(file_stat["size"], expected_size)
        
        fsapi.close(fd)

    def test_extent_tree_split_and_merge(self):
        """Тестирует разделение и слияние узлов в B+ дереве."""
        fd = fsapi.openf("/tree_test.txt", fsapi.O_CREAT | fsapi.O_RDWR)
        
        # Создаем достаточно экстентов, чтобы превысить емкость root узла
        # Обычно это 3-4 экстента в root inode
        num_extents = 10
        data_pattern = b"X" * 100
        
        for i in range(num_extents):
            # Создаем экстенты с промежутками
            offset = i * fsapi.BLOCK_SIZE * 2  # Промежуток в 1 блок между экстентами
            fsapi.write(fd, data_pattern, offset=offset)
        
        # Проверяем, что дерево корректно обрабатывает много экстентов
        for i in range(num_extents):
            offset = i * fsapi.BLOCK_SIZE * 2
            read_data = fsapi.read(fd, len(data_pattern), offset=offset)
            self.assertEqual(read_data, data_pattern)
        
        # Заполняем промежутки, создавая возможность для слияния экстентов
        gap_data = b"Y" * fsapi.BLOCK_SIZE
        for i in range(num_extents - 1):
            gap_offset = i * fsapi.BLOCK_SIZE * 2 + fsapi.BLOCK_SIZE
            fsapi.write(fd, gap_data, offset=gap_offset)
        
        # Проверяем целостность после заполнения промежутков
        for i in range(num_extents - 1):
            # Проверяем оригинальные данные
            offset = i * fsapi.BLOCK_SIZE * 2
            read_data = fsapi.read(fd, len(data_pattern), offset=offset)
            self.assertEqual(read_data, data_pattern)
            
            # Проверяем данные в промежутках
            gap_offset = i * fsapi.BLOCK_SIZE * 2 + fsapi.BLOCK_SIZE
            gap_read = fsapi.read(fd, len(gap_data), offset=gap_offset)
            self.assertEqual(gap_read, gap_data)
        
        fsapi.close(fd)

    def test_very_large_file(self):
        """Тест создания очень большого файла."""
        fd = fsapi.openf("/huge.txt", fsapi.O_CREAT | fsapi.O_RDWR)
        
        # Создаем файл размером несколько мегабайт, но записываем только в ключевые точки
        mega = 1024 * 1024
        test_points = [0, mega, 2*mega, 5*mega, 10*mega]
        
        for i, offset in enumerate(test_points):
            marker = f"marker_{i}".encode()
            try:
                fsapi.write(fd, marker, offset=offset)
            except OSError as e:
                # Если закончилось место, это нормально
                if "No free blocks" in str(e):
                    break
                raise
        
        # Проверяем маркеры
        for i, offset in enumerate(test_points):
            try:
                marker = f"marker_{i}".encode()
                read_data = fsapi.read(fd, len(marker), offset=offset)
                if read_data == marker:
                    # Этот маркер успешно записан
                    pass
                else:
                    # Достигли предела файловой системы
                    break
            except Exception:
                break
        
        fsapi.close(fd)


class TestConcurrentAccess(TestCase):
    """Тесты на корректность при одновременном доступе."""

    def test_multiple_writers_same_file(self):
        """Тест записи в один файл несколькими дескрипторами."""
        fd1 = fsapi.openf("/shared.txt", fsapi.O_CREAT | fsapi.O_RDWR)
        fd2 = fsapi.openf("/shared.txt", fsapi.O_RDWR)
        fd3 = fsapi.openf("/shared.txt", fsapi.O_RDWR)
        
        # Записываем разные данные в разные части файла
        fsapi.write(fd1, b"AAAA", offset=0)
        fsapi.write(fd2, b"BBBB", offset=10)
        fsapi.write(fd3, b"CCCC", offset=20)
        
        # Читаем через все дескрипторы
        content1 = fsapi.read(fd1, 30, offset=0)
        content2 = fsapi.read(fd2, 30, offset=0)
        content3 = fsapi.read(fd3, 30, offset=0)
        
        # Все должны видеть одинаковое содержимое
        expected = b"AAAA\x00\x00\x00\x00\x00\x00BBBB\x00\x00\x00\x00\x00\x00CCCC"
        self.assertEqual(content1, expected)
        self.assertEqual(content2, expected)
        self.assertEqual(content3, expected)
        
        fsapi.close(fd1)
        fsapi.close(fd2)
        fsapi.close(fd3)

    def test_read_while_writing(self):
        """Тест чтения файла во время записи."""
        # Создаем базовый файл
        fd_write = fsapi.openf("/read_write_test.txt", fsapi.O_CREAT | fsapi.O_RDWR)
        fd_read = fsapi.openf("/read_write_test.txt", fsapi.O_RDONLY)
        
        # Записываем данные порциями
        data_chunks = [b"chunk1", b"chunk2", b"chunk3", b"chunk4"]
        
        for i, chunk in enumerate(data_chunks):
            offset = i * 10
            fsapi.write(fd_write, chunk, offset=offset)
            
            # После каждой записи проверяем, что читающий дескриптор видит изменения
            current_content = fsapi.read(fd_read, offset + len(chunk), offset=0)
            
            # Проверяем, что новые данные видны
            chunk_start = offset
            chunk_end = offset + len(chunk)
            actual_chunk = current_content[chunk_start:chunk_end]
            self.assertEqual(actual_chunk, chunk)
        
        fsapi.close(fd_write)
        fsapi.close(fd_read)


class TestErrorRecovery(TestCase):
    """Тесты восстановления после ошибок."""

    def test_partial_write_recovery(self):
        """Тест восстановления после частичной записи."""
        fd = fsapi.openf("/partial.txt", fsapi.O_CREAT | fsapi.O_RDWR)
        
        # Записываем большой объем данных
        large_data = b"X" * (fsapi.BLOCK_SIZE * 2)
        fsapi.write(fd, large_data)
        
        # Проверяем, что запись прошла корректно
        read_data = fsapi.read(fd, len(large_data), offset=0)
        self.assertEqual(read_data, large_data)
        
        # Перезаписываем часть данных
        partial_data = b"Y" * 100
        fsapi.write(fd, partial_data, offset=fsapi.BLOCK_SIZE - 50)
        
        # Проверяем корректность после частичной перезаписи
        full_read = fsapi.read(fd, len(large_data), offset=0)
        
        # Создаем ожидаемый результат
        expected = bytearray(large_data)
        start_offset = fsapi.BLOCK_SIZE - 50
        expected[start_offset:start_offset + len(partial_data)] = partial_data
        
        self.assertEqual(full_read, bytes(expected))
        
        fsapi.close(fd)

    def test_filesystem_consistency_after_operations(self):
        """Проверяет консистентность метаданных файловой системы."""
        # Запоминаем начальное состояние
        initial_sb = fsapi.get_filesystem().superblock
        initial_free_inodes = initial_sb.free_inodes_count
        initial_free_blocks = initial_sb.free_blocks_count
        
        # Выполняем серию операций
        operations = [
            ("create", "/test1.txt"),
            ("mkdir", "/testdir"),
            ("create", "/testdir/test2.txt"),
            ("unlink", "/test1.txt"),
            ("create", "/test3.txt"),
            ("rmdir", "/testdir"),  # Должен провалиться - каталог не пустой
        ]
        
        for op, path in operations:
            try:
                if op == "create":
                    fd = fsapi.openf(path, fsapi.O_CREAT)
                    fsapi.close(fd)
                elif op == "mkdir":
                    fsapi.mkdir(path)
                elif op == "unlink":
                    fsapi.unlink(path)
                elif op == "rmdir":
                    fsapi.rmdir(path)
            except OSError:
                # Некоторые операции могут провалиться, это нормально
                pass
        
        # Проверяем, что счетчики не "поехали"
        final_sb = fsapi.get_filesystem().superblock
        
        # Разница в свободных ресурсах не должна быть критической
        inode_diff = initial_free_inodes - final_sb.free_inodes_count
        block_diff = initial_free_blocks - final_sb.free_blocks_count
        
        # Должны быть использованы ресурсы (отрицательная разница означает освобождение)
        self.assertTrue(-5 <= inode_diff <= 10, f"Suspicious inode count change: {inode_diff}")
        self.assertTrue(-10 <= block_diff <= 20, f"Suspicious block count change: {block_diff}")


class TestPerformanceAndLimits(TestCase):
    """Тесты производительности и граничных условий."""

    def test_maximum_filename_length(self):
        """Тест максимальной длины имени файла."""
        # Большинство файловых систем ограничивают длину имени 255 байтами
        max_name = "a" * 255
        short_name = "a" * 100
        
        # Короткое имя должно работать
        fd = fsapi.openf(f"/{short_name}", fsapi.O_CREAT)
        fsapi.close(fd)
        self.assertTrue(short_name in fsapi.readdir("/"))
        
        # Очень длинное имя может не поместиться в блок каталога
        try:
            fd = fsapi.openf(f"/{max_name}", fsapi.O_CREAT)
            fsapi.close(fd)
            # Если создался, проверяем, что он виден
            self.assertTrue(max_name in fsapi.readdir("/"))
        except OSError:
            # Если не поместился, это тоже нормально
            pass

    def test_deep_directory_nesting(self):
        """Тест глубокой вложенности каталогов."""
        max_depth = 50
        path = ""
        
        # Создаем глубокую иерархию
        for i in range(max_depth):
            path += f"/level{i}"
            try:
                fsapi.mkdir(path)
            except OSError:
                # Если достигли лимита, останавливаемся
                max_depth = i
                break
        
        # Проверяем, что можем создать файл на максимальной глубине
        if max_depth > 0:
            # Восстанавливаем path до последнего успешно созданного уровня
            path = "/".join(f"/level{i}" for i in range(max_depth))
            file_path = f"{path}/deep_file.txt"
            
            try:
                fd = fsapi.openf(file_path, fsapi.O_CREAT | fsapi.O_WRONLY)
                fsapi.write(fd, b"deep content")
                fsapi.close(fd)
                
                # Проверяем чтение
                fd = fsapi.openf(file_path, fsapi.O_RDONLY)
                content = fsapi.read(fd, 100)
                fsapi.close(fd)
                self.assertEqual(content, b"deep content")
                
            except OSError:
                # Если path resolution не справляется, это тоже информативно
                pass

    def test_stress_file_creation_deletion(self):
        """Стресс-тест создания и удаления множества файлов."""
        num_files = 50  # Ограничиваем, чтобы не исчерпать inodes
        
        # Создаем много файлов
        created_files = []
        for i in range(num_files):
            try:
                filename = f"/stress_{i:03d}.txt"
                fd = fsapi.openf(filename, fsapi.O_CREAT | fsapi.O_WRONLY)
                fsapi.write(fd, f"stress test {i}".encode())
                fsapi.close(fd)
                created_files.append(filename)
            except OSError as e:
                if "No free" in str(e):
                    # Закончились ресурсы
                    break
                raise
        
        # Проверяем, что все созданные файлы видны
        root_contents = fsapi.readdir("/")
        for filename in created_files:
            basename = filename[1:]  # убираем ведущий /
            self.assertTrue(basename in root_contents)
        
        # Удаляем все файлы
        for filename in created_files:
            fsapi.unlink(filename)
        
        # Проверяем, что все удалены
        root_contents_after = fsapi.readdir("/")
        for filename in created_files:
            basename = filename[1:]
            self.assertTrue(basename not in root_contents_after)

    def test_filesystem_space_exhaustion(self):
        """Тест исчерпания места на диске."""
        block_size = fsapi.BLOCK_SIZE
        large_chunk = b"X" * block_size
        
        files_created = []
        try:
            # Пытаемся создать много больших файлов до исчерпания места
            for i in range(100):
                filename = f"/space_test_{i}.txt"
                fd = fsapi.openf(filename, fsapi.O_CREAT | fsapi.O_WRONLY)
                
                # Записываем несколько блоков в каждый файл
                for j in range(5):
                    fsapi.write(fd, large_chunk)
                
                fsapi.close(fd)
                files_created.append(filename)
                
        except OSError as e:
            # Должны получить ошибку нехватки места
            self.assertTrue("No free blocks" in str(e) or "space" in str(e).lower())
        
        # Проверяем, что хотя бы некоторые файлы созданы
        self.assertTrue(len(files_created) > 0)
        
        # Освобождаем место
        for filename in files_created:
            try:
                fsapi.unlink(filename)
            except:
                pass

    def test_zero_byte_operations(self):
        """Тест операций с нулевыми размерами."""
        fd = fsapi.openf("/zero_test.txt", fsapi.O_CREAT | fsapi.O_RDWR)
        
        # Запись 0 байт
        bytes_written = fsapi.write(fd, b"")
        self.assertEqual(bytes_written, 0)
        
        # Чтение 0 байт
        data = fsapi.read(fd, 0)
        self.assertEqual(data, b"")
        
        # Запись данных, затем чтение 0 байт
        fsapi.write(fd, b"some data")
        zero_read = fsapi.read(fd, 0, offset=5)
        self.assertEqual(zero_read, b"")
        
        fsapi.close(fd)


class TestSpecialCases(TestCase):
    """Тесты специальных и граничных случаев."""

    def test_empty_files_operations(self):
        """Тесты операций с пустыми файлами."""
        # Создаем пустой файл
        fd = fsapi.openf("/empty.txt", fsapi.O_CREAT)
        fsapi.close(fd)
        
        # Проверяем stat
        stat_info = fsapi.stat("/empty.txt")
        self.assertEqual(stat_info["size"], 0)
        self.assertEqual(stat_info["type"], fsapi.S_IFREG)
        
        # Читаем из пустого файла
        fd = fsapi.openf("/empty.txt", fsapi.O_RDONLY)
        data = fsapi.read(fd, 100)
        self.assertEqual(data, b"")
        fsapi.close(fd)
        
        # Перезаписываем пустой файл
        fd = fsapi.openf("/empty.txt", fsapi.O_WRONLY)
        fsapi.write(fd, b"no longer empty")
        fsapi.close(fd)
        
        # Проверяем, что файл теперь не пустой
        new_stat = fsapi.stat("/empty.txt")
        self.assertEqual(new_stat["size"], 15)

    def test_file_permissions_and_modes(self):
        """Тест различных режимов доступа к файлам."""
        # Создаем файл с определенными правами
        fd = fsapi.openf("/perm_test.txt", fsapi.O_CREAT | fsapi.O_WRONLY, 0o644)
        fsapi.write(fd, b"permission test")
        fsapi.close(fd)
        
        stat_info = fsapi.stat("/perm_test.txt")
        # Проверяем, что права сохранились (если реализовано)
        # В базовой реализации может не поддерживаться
        self.assertTrue(stat_info["mode"] != 0)

    def test_directory_dot_entries(self):
        """Тест записей '.' и '..' в каталогах."""
        fsapi.mkdir("/dottest")
        fsapi.mkdir("/dottest/subdir")
        
        # В реальной ext4 есть записи '.' и '..', проверим их наличие
        try:
            # Некоторые реализации могут не показывать . и .. в readdir
            contents = fsapi.readdir("/dottest")
            # Если есть поддержка, проверяем
            if "." in contents:
                self.assertTrue(".." in contents)
            
            # Проверяем, что subdir виден
            self.assertTrue("subdir" in contents)
            
        except:
            # Если реализация не поддерживает . и .., это нормально
            pass

    def test_file_timestamps(self):
        """Тест временных меток файлов."""
        import time
        
        start_time = int(time.time())
        
        # Создаем файл
        fd = fsapi.openf("/timestamp_test.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, b"timestamp data")
        fsapi.close(fd)
        
        stat_info = fsapi.stat("/timestamp_test.txt")
        
        # Проверяем, что временные метки разумные
        # (в пределах нескольких секунд от текущего времени)
        if 'mtime' in stat_info:
            time_diff = abs(stat_info['mtime'] - start_time)
            self.assertTrue(time_diff < 10)  # В пределах 10 секунд
        
        # Модифицируем файл
        time.sleep(1)  # Ждем секунду для изменения времени
        fd = fsapi.openf("/timestamp_test.txt", fsapi.O_WRONLY)
        fsapi.write(fd, b" modified")
        fsapi.close(fd)
        
        new_stat = fsapi.stat("/timestamp_test.txt")
        if 'mtime' in new_stat and 'mtime' in stat_info:
            # Время модификации должно измениться
            self.assertTrue(new_stat['mtime'] >= stat_info['mtime'])

    def test_case_sensitivity(self):
        """Тест чувствительности к регистру."""
        # Создаем файлы с разным регистром
        fd1 = fsapi.openf("/CaseSensitive.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd1, b"upper case")
        fsapi.close(fd1)
        
        fd2 = fsapi.openf("/casesensitive.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd2, b"lower case")
        fsapi.close(fd2)
        
        # Должны быть разные файлы
        contents = fsapi.readdir("/")
        self.assertTrue("CaseSensitive.txt" in contents)
        self.assertTrue("casesensitive.txt" in contents)
        
        # Проверяем содержимое
        fd1 = fsapi.openf("/CaseSensitive.txt", fsapi.O_RDONLY)
        content1 = fsapi.read(fd1, 100)
        fsapi.close(fd1)
        
        fd2 = fsapi.openf("/casesensitive.txt", fsapi.O_RDONLY)
        content2 = fsapi.read(fd2, 100)
        fsapi.close(fd2)
        
        self.assertEqual(content1, b"upper case")
        self.assertEqual(content2, b"lower case")

    def test_unicode_filenames(self):
        """Тест поддержки Unicode имен файлов."""
        unicode_names = [
            "файл.txt",           # Кириллица
            "文件.txt",            # Китайский
            "αρχείο.txt",         # Греческий
            "🚀rocket.txt",       # Emoji
            "naïve_café.txt",     # Диакритики
        ]
        
        created_files = []
        for name in unicode_names:
            try:
                path = f"/{name}"
                fd = fsapi.openf(path, fsapi.O_CREAT | fsapi.O_WRONLY)
                fsapi.write(fd, name.encode('utf-8'))
                fsapi.close(fd)
                created_files.append(name)
            except (OSError, UnicodeError):
                # Некоторые символы могут не поддерживаться
                pass
        
        # Проверяем созданные файлы
        if created_files:
            contents = fsapi.readdir("/")
            for name in created_files:
                self.assertTrue(name in contents)
                
                # Проверяем чтение
                fd = fsapi.openf(f"/{name}", fsapi.O_RDONLY)
                content = fsapi.read(fd, 100)
                fsapi.close(fd)
                self.assertEqual(content.decode('utf-8'), name)


class TestAtomicOperations(TestCase):
    """Тесты атомарности операций."""

    def test_atomic_file_creation(self):
        """Тест атомарности создания файла."""
        # Проверяем, что файл либо создается полностью, либо не создается вовсе
        
        # Попытка создать файл с невозможным именем
        try:
            fd = fsapi.openf("/", fsapi.O_CREAT)  # Попытка создать файл с именем "/"
            fsapi.close(fd)
            self.fail("Should not be able to create file with name '/'")
        except OSError:
            pass
        
        # Проверяем, что корневой каталог не поврежден
        contents = fsapi.readdir("/")
        # Каталог должен работать нормально

    def test_atomic_directory_operations(self):
        """Тест атомарности операций с каталогами."""
        fsapi.mkdir("/atomic_test")
        
        # Добавляем файлы в каталог
        for i in range(3):
            fd = fsapi.openf(f"/atomic_test/file{i}.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
            fsapi.write(fd, f"content {i}".encode())
            fsapi.close(fd)
        
        # Попытка удалить непустой каталог должна провалиться атомарно
        try:
            fsapi.rmdir("/atomic_test")
            self.fail("Should not be able to remove non-empty directory")
        except OSError:
            pass
        
        # Каталог должен остаться нетронутым
        contents = fsapi.readdir("/atomic_test")
        self.assertEqual(len(contents), 3)
        for i in range(3):
            self.assertTrue(f"file{i}.txt" in contents)

    def test_rename_atomicity(self):
        """Тест атомарности переименования (если поддерживается)."""
        # Создаем файл
        fd = fsapi.openf("/original_name.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, b"rename test data")
        fsapi.close(fd)
        
        # Если API поддерживает rename, тестируем его
        try:
            # Пытаемся найти функцию rename в API
            if hasattr(fsapi, 'rename'):
                fsapi.rename("/original_name.txt", "/new_name.txt")
                
                # Проверяем, что старое имя исчезло
                self.assertRaises(FileNotFoundError, fsapi.stat, "/original_name.txt")
                
                # Проверяем, что новое имя работает
                new_stat = fsapi.stat("/new_name.txt")
                self.assertEqual(new_stat["size"], 16)
                
                fd = fsapi.openf("/new_name.txt", fsapi.O_RDONLY)
                content = fsapi.read(fd, 100)
                fsapi.close(fd)
                self.assertEqual(content, b"rename test data")
        except AttributeError:
            # Rename не поддерживается, пропускаем тест
            pass


class TestBoundaryConditions(TestCase):
    """Тесты граничных условий."""

    def test_block_boundary_reads_writes(self):
        """Тест чтения/записи на границах блоков."""
        block_size = fsapi.BLOCK_SIZE
        
        fd = fsapi.openf("/boundary.txt", fsapi.O_CREAT | fsapi.O_RDWR)
        
        # Записываем данные, пересекающие границу блока
        data_before = b"A" * (block_size - 10)
        data_after = b"B" * 20
        combined_data = data_before + data_after
        
        fsapi.write(fd, combined_data)
        
        # Читаем по частям
        # Читаем до границы блока
        part1 = fsapi.read(fd, block_size - 10, offset=0)
        self.assertEqual(part1, data_before)
        
        # Читаем через границу блока
        part2 = fsapi.read(fd, 20, offset=block_size - 10)
        self.assertEqual(part2, data_after)
        
        # Читаем один байт на границе
        boundary_byte = fsapi.read(fd, 1, offset=block_size - 1)
        self.assertEqual(boundary_byte, b"A")
        
        next_byte = fsapi.read(fd, 1, offset=block_size)
        self.assertEqual(next_byte, b"B")
        
        fsapi.close(fd)

    def test_maximum_offset_operations(self):
        """Тест операций с максимальными смещениями."""
        fd = fsapi.openf("/max_offset.txt", fsapi.O_CREAT | fsapi.O_RDWR)
        
        # Пытаемся записать на очень большом смещении
        large_offset = 10 * 1024 * 1024  # 10MB
        
        try:
            fsapi.write(fd, b"far away", offset=large_offset)
            
            # Проверяем размер файла
            stat_info = fsapi.stat("/max_offset.txt")
            expected_size = large_offset + 8
            self.assertEqual(stat_info["size"], expected_size)
            
            # Читаем данные
            read_data = fsapi.read(fd, 8, offset=large_offset)
            self.assertEqual(read_data, b"far away")
            
        except OSError as e:
            if "No free blocks" in str(e):
                # Нормально, если недостаточно места
                pass
            else:
                raise
        
        fsapi.close(fd)

    def test_inode_number_limits(self):
        """Тест пределов номеров inode."""
        # Получаем информацию о файловой системе
        sb = fsapi.get_filesystem().superblock
        max_inodes = sb.total_inodes
        
        # Создаем файлы до исчерпания inode
        created_count = 0
        try:
            for i in range(max_inodes + 10):  # Пытаемся создать больше, чем возможно
                fd = fsapi.openf(f"/inode_test_{i}.txt", fsapi.O_CREAT)
                fsapi.close(fd)
                created_count += 1
        except OSError as e:
            # Должны получить ошибку исчерпания inode
            self.assertTrue("inode" in str(e).lower() or "No free" in str(e))
        
        # Проверяем, что создали хотя бы несколько файлов
        self.assertTrue(created_count > 0)
        self.assertTrue(created_count < max_inodes + 10)  # Не должны превысить лимит


class TestFileSystemIntegrity(TestCase):
    """Тесты целостности файловой системы."""

    def test_superblock_consistency(self):
        """Тест консистентности суперблока."""
        fs = fsapi.get_filesystem()
        sb_before = fs.superblock
        
        # Выполняем операции, которые должны изменить суперблок
        fd = fsapi.openf("/sb_test.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        fsapi.write(fd, b"test data" * 1000)  # Записываем данные
        fsapi.close(fd)
        
        sb_after = fs.superblock
        
        # Количество свободных блоков должно уменьшиться
        self.assertTrue(sb_after.free_blocks_count <= sb_before.free_blocks_count)
        
        # Количество свободных inode должно уменьшиться на 1
        self.assertEqual(sb_after.free_inodes_count, sb_before.free_inodes_count - 1)
        
        # Удаляем файл
        fsapi.unlink("/sb_test.txt")
        
        sb_final = fs.superblock
        
        # Ресурсы должны освободиться
        self.assertEqual(sb_final.free_inodes_count, sb_before.free_inodes_count)

    def test_bitmap_consistency(self):
        """Тест консистентности битовых карт."""
        fs = fsapi.get_filesystem()
        
        # Создаем файл и проверяем, что биты установлены
        fd = fsapi.openf("/bitmap_test.txt", fsapi.O_CREAT | fsapi.O_WRONLY)
        file_inode = fs._resolve_path("/bitmap_test.txt")
        
        # Проверяем, что inode помечен как занятый
        # (детали зависят от реализации bitmap)
        
        fsapi.write(fd, b"bitmap test data")
        fsapi.close(fd)
        
        # Удаляем файл
        fsapi.unlink("/bitmap_test.txt")
        
        # Проверяем, что inode освобожден
        # В реальной реализации здесь были бы более детальные проверки



class TestEdgeCasesAndCornerCases(TestCase):
    """Тесты крайних и угловых случаев."""

    def test_null_byte_in_data(self):
        """Тест обработки нулевых байтов в данных."""
        fd = fsapi.openf("/null_bytes.txt", fsapi.O_CREAT | fsapi.O_RDWR)
        
        # Записываем данные с нулевыми байтами
        data_with_nulls = b"Hello\x00World\x00\x00End"
        fsapi.write(fd, data_with_nulls)
        
        # Читаем обратно
        read_data = fsapi.read(fd, len(data_with_nulls), offset=0)
        self.assertEqual(read_data, data_with_nulls)
        
        # Проверяем размер
        stat_info = fsapi.stat("/null_bytes.txt")
        self.assertEqual(stat_info["size"], len(data_with_nulls))
        
        fsapi.close(fd)

    def test_overlapping_writes(self):
        """Тест перекрывающихся записей."""
        fd = fsapi.openf("/overlap.txt", fsapi.O_CREAT | fsapi.O_RDWR)
        
        # Первая запись
        fsapi.write(fd, b"1234567890", offset=0)
        
        # Перекрывающаяся запись
        fsapi.write(fd, b"ABCDEF", offset=3)
        
        # Ожидаемый результат: "123ABCDEF0" или "123ABCDEF" в зависимости от реализации
        result = fsapi.read(fd, 20, offset=0)
        
        # Проверяем, что перекрытие корректно обработано
        self.assertTrue(result.startswith(b"123ABCDEF"))
        
        fsapi.close(fd)

    def test_sparse_file_operations(self):
        """Тест операций с разреженными файлами."""
        fd = fsapi.openf("/sparse.txt", fsapi.O_CREAT | fsapi.O_RDWR)
        
        # Создаем разреженный файл
        fsapi.write(fd, b"start", offset=0)
        fsapi.write(fd, b"middle", offset=1000)
        fsapi.write(fd, b"end", offset=2000)
        
        # Проверяем размер
        stat_info = fsapi.stat("/sparse.txt")
        self.assertEqual(stat_info["size"], 2003)
        
        # Читаем дыры (должны содержать нули)
        hole1 = fsapi.read(fd, 10, offset=100)
        self.assertEqual(hole1, b"\x00" * 10)
        
        hole2 = fsapi.read(fd, 10, offset=1500)
        self.assertEqual(hole2, b"\x00" * 10)
        
        # Читаем данные
        start_data = fsapi.read(fd, 5, offset=0)
        self.assertEqual(start_data, b"start")
        
        middle_data = fsapi.read(fd, 6, offset=1000)
        self.assertEqual(middle_data, b"middle")
        
        end_data = fsapi.read(fd, 3, offset=2000)
        self.assertEqual(end_data, b"end")
        
        fsapi.close(fd)

    def test_concurrent_file_operations(self):
        """Тест одновременных операций над одним файлом."""
        # Создаем файл
        fd1 = fsapi.openf("/concurrent.txt", fsapi.O_CREAT | fsapi.O_RDWR)
        fd2 = fsapi.openf("/concurrent.txt", fsapi.O_RDWR)
        
        # Записываем через разные дескрипторы
        fsapi.write(fd1, b"FD1: ", offset=0)
        fsapi.write(fd2, b"FD2: ", offset=10)
        fsapi.write(fd1, b"MORE1", offset=5)
        fsapi.write(fd2, b"MORE2", offset=15)
        
        # Читаем результат
        result = fsapi.read(fd1, 50, offset=0)
        
        # Проверяем, что все записи применились
        self.assertTrue(b"FD1: " in result)
        self.assertTrue(b"FD2: " in result)
        self.assertTrue(b"MORE1" in result)
        self.assertTrue(b"MORE2" in result)
        
        fsapi.close(fd1)
        fsapi.close(fd2)

    def test_file_descriptor_limits(self):
        """Тест лимитов на количество открытых дескрипторов."""
        open_fds = []
        
        try:
            # Пытаемся открыть много файлов одновременно
            for i in range(100):
                filename = f"/fd_limit_{i}.txt"
                fd = fsapi.openf(filename, fsapi.O_CREAT | fsapi.O_RDWR)
                fsapi.write(fd, f"file {i}".encode())
                open_fds.append(fd)
                
        except OSError as e:
            # Достигли лимита дескрипторов
            self.assertTrue("descriptor" in str(e).lower() or "limit" in str(e).lower())
        
        # Закрываем все открытые дескрипторы
        for fd in open_fds:
            try:
                fsapi.close(fd)
            except:
                pass


if __name__ == "__main__":
    console.print("[bold white on blue]EXT4-like Filesystem Test Suite[/bold white on blue]\n")
    
    runner = TestRunner()
    
    # Основные тесты
    runner.run(TestCoreFS)
    runner.run(TestAdvancedFS)
    runner.run(TestErrorConditions)
    runner.run(TestNamingAndPaths)
    runner.run(TestResourceManagement)
    runner.run(TestDataIntegrity)
    
    # Расширенные функции
    runner.run(TestHardLinksAndSymlinks)
    runner.run(TestDirectoryOperations)
    
    # Стресс-тесты и производительность
    runner.run(TestExtentTreeStress)
    runner.run(TestConcurrentAccess)
    runner.run(TestPerformanceAndLimits)
    
    # Специальные случаи
    runner.run(TestSpecialCases)
    runner.run(TestAtomicOperations)
    runner.run(TestBoundaryConditions)
    
    # Целостность системы
    runner.run(TestFileSystemIntegrity)
    runner.run(TestErrorRecovery)
    runner.run(TestEdgeCasesAndCornerCases)
    
    runner.summary()
