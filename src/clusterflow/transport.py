"""Verified SSH, system-only credential storage and atomic SFTP writes."""

import base64
import errno
import hashlib
import os
from pathlib import PurePosixPath
import shlex
import socket
import stat
import sys
import time
import uuid

from .storage import FlowError, digest


def credential_id(profile):
    identity = f"{profile['host']}:{profile['port']}:{profile['user']}"
    return f"{profile['name']}/{digest(identity.encode())[:24]}"


def credential_backend():
    try:
        if sys.platform == "win32":
            from keyring.backends.Windows import WinVaultKeyring
            backend = WinVaultKeyring()
        elif sys.platform == "darwin":
            from keyring.backends.macOS import Keyring
            backend = Keyring()
        else:
            from keyring.backends.SecretService import Keyring
            backend = Keyring()
        if backend.priority <= 0:
            raise RuntimeError("Unavailable keyring")
        return backend
    except Exception as error:
        raise FlowError("A supported system credential store is unavailable. Configure it or use SSH agent/key authentication; plaintext fallback is forbidden.") from error


def password_get(profile):
    try:
        value = credential_backend().get_password("clusterflow", credential_id(profile))
    except FlowError:
        raise
    except Exception as error:
        raise FlowError("System credential lookup failed; unlock the credential store locally.") from error
    if not value:
        raise FlowError("No saved credential. Run 'auth set CLUSTER' in an interactive local terminal.")
    return value


def password_set(profile, value):
    try:
        credential_backend().set_password("clusterflow", credential_id(profile), value)
    except FlowError:
        raise
    except Exception as error:
        raise FlowError("System credential storage failed; no plaintext password was saved.") from error


def fingerprint(key):
    return "SHA256:" + base64.b64encode(hashlib.sha256(key.asbytes()).digest()).decode().rstrip("=")


def host_label(profile):
    return profile["host"] if profile["port"] == 22 else f"[{profile['host']}]:{profile['port']}"


def probe_key(store, profile):
    import paramiko
    jump = None
    connection = None
    try:
        if profile.get("jump"):
            jump = Remote(store, store.get("clusters", profile["jump"]))
            connection = jump.client.get_transport().open_channel("direct-tcpip", (profile["host"], profile["port"]), ("127.0.0.1", 0))
        else:
            connection = socket.create_connection((profile["host"], profile["port"]), timeout=15)
        with paramiko.Transport(connection) as transport:
            transport.start_client(timeout=15)
            return transport.get_remote_server_key()
    finally:
        if connection:
            connection.close()
        if jump:
            jump.close()


def trust_key(store, profile, key, expected):
    import paramiko
    if expected != fingerprint(key):
        raise FlowError("Server fingerprint does not match the explicitly verified fingerprint.")
    file = store.home / "known_hosts"
    keys = paramiko.HostKeys()
    if file.exists():
        keys.load(str(file))
    existing = keys.lookup(host_label(profile))
    if existing and (key.get_name() not in existing or existing[key.get_name()] != key):
        raise FlowError("Stored host key changed. Verify with the administrator; automatic replacement is forbidden.")
    keys.add(host_label(profile), key.get_name(), key)
    store.home.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = store.home / f"known_hosts.{uuid.uuid4().hex}.tmp"
    try:
        keys.save(str(temporary))
        os.chmod(temporary, 0o600)
        os.replace(temporary, file)
    finally:
        temporary.unlink(missing_ok=True)


class Remote:
    def __init__(self, store, profile, password=None, chain=()):
        import paramiko
        if profile["name"] in chain or len(chain) > 3:
            raise FlowError("Jump-host cycle or excessive chain depth.")
        self.profile = profile
        self.jump = None
        self.sftp = None
        self.client = paramiko.SSHClient()
        try:
            self.client.load_host_keys(str(store.home / "known_hosts"))
            self.client.set_missing_host_key_policy(paramiko.RejectPolicy())
            options = dict(hostname=profile["host"], port=profile["port"], username=profile["user"], timeout=15, auth_timeout=20, banner_timeout=20)
            if profile.get("jump"):
                self.jump = Remote(store, store.get("clusters", profile["jump"]), chain=(*chain, profile["name"]))
                options["sock"] = self.jump.client.get_transport().open_channel("direct-tcpip", (profile["host"], profile["port"]), ("127.0.0.1", 0))
            if profile["auth"] == "password":
                options.update(password=password if password is not None else password_get(profile), allow_agent=False, look_for_keys=False)
            elif profile["auth"] == "key":
                options.update(key_filename=profile["key_file"], allow_agent=False, look_for_keys=False)
            else:
                options.update(allow_agent=True, look_for_keys=False)
            self.client.connect(**options)
            self.sftp = self.client.open_sftp()
        except Exception as error:
            self.close()
            if isinstance(error, FlowError):
                raise
            raise FlowError(f"Verified SSH connection failed ({type(error).__name__}); check network, host key and authentication locally.") from error

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def close(self):
        if self.sftp:
            self.sftp.close()
        self.client.close()
        if self.jump:
            self.jump.close()

    def exec(self, arguments, timeout=60):
        command = shlex.join([str(value) for value in arguments])
        channel = self.client.get_transport().open_session(timeout=15)
        out, err = bytearray(), bytearray()
        started = time.monotonic()
        try:
            channel.exec_command(command)
            while True:
                while channel.recv_ready():
                    out.extend(channel.recv(65536))
                while channel.recv_stderr_ready():
                    err.extend(channel.recv_stderr(65536))
                if len(out) + len(err) > 4 * 1024 * 1024:
                    raise FlowError("Remote response exceeded the safety limit.")
                if channel.exit_status_ready() and not channel.recv_ready() and not channel.recv_stderr_ready():
                    return channel.recv_exit_status(), out.decode("utf-8", "replace"), err.decode("utf-8", "replace")
                if time.monotonic() - started > timeout:
                    raise FlowError("Remote command timed out; a submission may still have reached Slurm. Reconcile before retrying.")
                if channel.closed:
                    raise FlowError("Remote channel closed before a confirmed exit status.")
                time.sleep(0.02)
        finally:
            channel.close()

    def exists(self, path):
        try:
            self.sftp.lstat(path)
            return True
        except OSError as error:
            if error.errno == errno.ENOENT:
                return False
            raise

    def mkdirs(self, path):
        current = "/"
        for part in PurePosixPath(path).parts[1:]:
            current = str(PurePosixPath(current) / part)
            if not self.exists(current):
                self.sftp.mkdir(current, mode=0o700)
            if not stat.S_ISDIR(self.sftp.lstat(current).st_mode):
                raise FlowError("Remote parent is not a real directory; symbolic links are not followed.")

    def read(self, path, limit=64 * 1024 * 1024):
        info = self.sftp.lstat(path)
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            raise FlowError("Remote file is not regular or exceeds the transfer size limit.")
        with self.sftp.open(path, "rb") as stream:
            data = stream.read(limit + 1)
        if len(data) > limit:
            raise FlowError("Remote file grew beyond the transfer limit.")
        return data

    def write(self, path, data, mode=0o600):
        self.mkdirs(str(PurePosixPath(path).parent))
        if self.exists(path):
            raise FlowError("Remote target already exists; refusing to overwrite.")
        temporary = path + ".part-" + uuid.uuid4().hex
        with self.sftp.open(temporary, "wb") as stream:
            stream.write(data)
            stream.flush()
        if digest(self.read(temporary, max(len(data), 1))) != digest(data):
            raise FlowError("Uploaded content checksum mismatch; target was not published.")
        self.sftp.chmod(temporary, mode)
        self.sftp.rename(temporary, path)

    def list(self, path):
        if not stat.S_ISDIR(self.sftp.lstat(path).st_mode):
            raise FlowError("Result group must be a real directory, not a file or symlink.")
        return self.sftp.listdir_attr(path)

    def seal(self, path):
        for item in self.list(path):
            child = path + "/" + item.filename
            if stat.S_ISDIR(item.st_mode):
                self.seal(child)
            elif not stat.S_ISREG(item.st_mode):
                raise FlowError("Unexpected non-regular item in a code snapshot.")
        self.sftp.chmod(path, 0o555)
