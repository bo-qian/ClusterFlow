"""Real transfer helper against a local fake SFTP filesystem (no sockets)."""

import os
from pathlib import Path, PurePosixPath
import stat
import sys
import tempfile
from types import SimpleNamespace
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from clusterflow.storage import FlowError
from clusterflow.transport import Remote


class LocalSFTP:
    def __init__(self, root):
        self.root = Path(root)
        self.corrupt = False

    def path(self, value):
        path = PurePosixPath(value)
        if not path.is_absolute() or ".." in path.parts:
            raise AssertionError("unsafe test path")
        return self.root.joinpath(*path.parts[1:])

    def lstat(self, value):
        return self.path(value).lstat()

    def mkdir(self, value, mode=0o700):
        self.path(value).mkdir(mode=mode)

    def open(self, value, mode):
        if self.corrupt and ".part-" in value and mode == "rb":
            import io
            return io.BytesIO(b"corrupt")
        return self.path(value).open(mode)

    def chmod(self, value, mode):
        self.path(value).chmod(mode)

    def rename(self, source, target):
        if self.path(target).exists():
            raise FileExistsError(target)
        self.path(source).rename(self.path(target))

    def listdir_attr(self, value):
        return [SimpleNamespace(filename=path.name, st_mode=path.lstat().st_mode, st_size=path.lstat().st_size) for path in self.path(value).iterdir()]


class TransportTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="clusterflow-sftp-")
        self.addCleanup(self.temporary.cleanup)
        self.remote = Remote.__new__(Remote)
        self.remote.sftp = LocalSFTP(self.temporary.name)

    def test_atomic_write_and_content_check(self):
        self.remote.write("/project/source/file.txt", b"correct")
        self.assertEqual(self.remote.read("/project/source/file.txt"), b"correct")
        self.assertFalse(any(".part-" in path.name for path in Path(self.temporary.name).rglob("*")))

    def test_checksum_failure_never_publishes_target(self):
        self.remote.sftp.corrupt = True
        with self.assertRaises(FlowError):
            self.remote.write("/project/file.txt", b"correct")
        self.assertFalse(self.remote.exists("/project/file.txt"))

    def test_existing_target_is_not_overwritten(self):
        self.remote.write("/project/file.txt", b"keep")
        with self.assertRaises(FlowError):
            self.remote.write("/project/file.txt", b"replace")
        self.assertEqual(self.remote.read("/project/file.txt"), b"keep")

    def test_remote_read_limit(self):
        self.remote.write("/project/file.txt", b"123456")
        with self.assertRaises(FlowError):
            self.remote.read("/project/file.txt", limit=3)

    def test_file_cannot_be_result_directory(self):
        self.remote.write("/project/file.txt", b"content")
        with self.assertRaises(FlowError):
            self.remote.list("/project/file.txt")

    @unittest.skipIf(os.name == "nt", "POSIX permission check")
    def test_code_directory_is_sealed_readonly(self):
        self.remote.write("/project/code/file.txt", b"content", mode=0o444)
        self.remote.seal("/project/code")
        self.assertEqual(self.remote.sftp.path("/project/code").stat().st_mode & 0o222, 0)
        self.remote.sftp.chmod("/project/code", 0o700)

    @unittest.skipIf(os.name == "nt", "requires unprivileged symlinks")
    def test_remote_symlink_parent_is_rejected(self):
        self.remote.mkdirs("/real")
        (Path(self.temporary.name) / "link").symlink_to(Path(self.temporary.name) / "real", target_is_directory=True)
        with self.assertRaises(FlowError):
            self.remote.write("/link/file", b"unsafe")


if __name__ == "__main__":
    unittest.main()
