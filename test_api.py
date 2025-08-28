#!/usr/bin/env python3
"""
Test script for the filesystem API
"""

import os
from main import mkfs
import fsapi
from rich.console import Console

console = Console()


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

    def assertRaises(self, exc_type, func, *args, **kwargs):
        try:
            func(*args, **kwargs)
        except exc_type:
            return  # Ожидаемое исключение поймано
        except Exception as e:
            raise AssertionError(f"Expected exception {exc_type.__name__}, but got {e.__class__.__name__}")
        raise AssertionError(f"Expected exception {exc_type.__name__}, but no exception was raised")


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


if __name__ == "__main__":
    console.print("[bold white on blue]EXT4-like Filesystem Test Suite[/bold white on blue]\n")
    
    runner = TestRunner()
    runner.run(TestCoreFS)
    runner.run(TestAdvancedFS)
    runner.run(TestErrorConditions)
    
    runner.summary()
