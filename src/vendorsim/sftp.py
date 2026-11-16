"""The distributor's SFTP drop, served by asyncssh from a directory of generated files."""

from __future__ import annotations

from pathlib import Path

import asyncssh

from vendorsim import data

SFTP_USER = "distributor"


def write_distributor_files(outbound: Path) -> int:
    outbound.mkdir(parents=True, exist_ok=True)
    n = 0
    for day in data.business_days():
        for name, payload in data.distributor_files(day).items():
            path = outbound / name
            if not path.exists():
                path.write_bytes(payload)
                n += 1
    return n


def host_key(secrets_dir: Path, state_dir: Path) -> asyncssh.SSHKey:
    """The server's host key stays in the simulator's state; only the public half is published, for pinning."""
    key_path = state_dir / "sftp_host_key"
    if key_path.exists():
        key = asyncssh.read_private_key(str(key_path))
    else:
        key = asyncssh.generate_private_key("ssh-ed25519")
        key.write_private_key(str(key_path))
    (secrets_dir / "sftp_host_key.pub").write_bytes(key.export_public_key())
    return key


async def start_sftp(root: Path, port: int, password: str, secrets_dir: Path, state_dir: Path):
    class Server(asyncssh.SSHServer):
        def begin_auth(self, username: str) -> bool:
            return True

        def password_auth_supported(self) -> bool:
            return True

        def validate_password(self, username: str, pw: str) -> bool:
            return username == SFTP_USER and pw == password

    def sftp_factory(chan):
        return asyncssh.SFTPServer(chan, chroot=str(root))

    return await asyncssh.listen("", port, server_factory=Server, server_host_keys=[host_key(secrets_dir, state_dir)],
                                 sftp_factory=sftp_factory, allow_scp=False)
