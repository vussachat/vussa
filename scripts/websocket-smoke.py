#!/usr/bin/env python3
"""Small standard-library WebSocket protocol probe for integration smoke tests."""

import base64
import hashlib
import json
import os
import random
import socket
import struct
import sys
import time
from urllib.parse import urlparse


class WebSocket:
    def __init__(self, url: str, cookie: str):
        parsed = urlparse(url)
        if parsed.scheme != "http":
            raise RuntimeError("smoke client currently requires an http endpoint")
        self.sock = socket.create_connection((parsed.hostname, parsed.port or 80), timeout=5)
        self.sock.settimeout(5)
        self.buffer = b""
        path = parsed.path or "/"
        key = base64.b64encode(os.urandom(16)).decode()
        request = (
            f"GET {path} HTTP/1.1\r\n"
            f"Host: {parsed.hostname}:{parsed.port or 80}\r\n"
            "Connection: Upgrade\r\n"
            "Upgrade: websocket\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            f"Cookie: {cookie}\r\n\r\n"
        ).encode()
        self.sock.sendall(request)
        response = self.read_until(b"\r\n\r\n")
        if not response.startswith(b"HTTP/1.1 101"):
            raise RuntimeError(f"WebSocket upgrade failed: {response[:120]!r}")

    def read_until(self, marker: bytes) -> bytes:
        while marker not in self.buffer:
            self.buffer += self.sock.recv(4096)
        end = self.buffer.index(marker) + len(marker)
        result, self.buffer = self.buffer[:end], self.buffer[end:]
        return result

    def read_exact(self, size: int) -> bytes:
        while len(self.buffer) < size:
            self.buffer += self.sock.recv(4096)
        result, self.buffer = self.buffer[:size], self.buffer[size:]
        return result

    def recv_text(self) -> dict:
        while True:
            first, second = self.read_exact(2)
            opcode = first & 0x0F
            length = second & 0x7F
            if length == 126:
                length = struct.unpack("!H", self.read_exact(2))[0]
            elif length == 127:
                length = struct.unpack("!Q", self.read_exact(8))[0]
            masked = second & 0x80
            mask = self.read_exact(4) if masked else b""
            payload = bytearray(self.read_exact(length))
            if masked:
                for index in range(length):
                    payload[index] ^= mask[index % 4]
            if opcode == 0x8:
                raise RuntimeError("server closed the WebSocket")
            if opcode == 0x1:
                return json.loads(payload.decode())

    def send_text(self, value: dict) -> None:
        payload = json.dumps(value, separators=(",", ":")).encode()
        mask = os.urandom(4)
        encoded = bytearray(payload)
        for index in range(len(encoded)):
            encoded[index] ^= mask[index % 4]
        length = len(encoded)
        if length < 126:
            header = bytes((0x81, 0x80 | length))
        elif length <= 0xFFFF:
            header = bytes((0x81, 0xFE)) + struct.pack("!H", length)
        else:
            header = bytes((0x81, 0xFF)) + struct.pack("!Q", length)
        self.sock.sendall(header + mask + encoded)

    def close(self) -> None:
        self.sock.close()


def main() -> int:
    url = sys.argv[1]
    cookie = sys.argv[2]
    client = WebSocket(url, cookie)
    try:
        deadline = time.monotonic() + 5
        welcome = None
        while time.monotonic() < deadline:
            event = client.recv_text()
            if event.get("type") == "welcome":
                welcome = event
                break
        if not welcome or not welcome.get("username"):
            raise RuntimeError("did not receive a valid welcome event")
        client_id = f"integration-{random.getrandbits(64):016x}"
        def wait_for(predicate, description):
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                event = client.recv_text()
                if predicate(event):
                    return event
            raise RuntimeError(f"did not receive {description}")

        target_channel = os.environ.get("SMOKE_CHANNEL")
        if target_channel:
            client.send_text({"type": "join_channel", "name": target_channel})
            wait_for(
                lambda event: event.get("type") == "joined" and event.get("name") == target_channel,
                "the requested channel join event",
            )

        message_text = os.environ.get("SMOKE_MESSAGE_TEXT", "websocket smoke message")
        file_ids = []
        if os.environ.get("SMOKE_FILE_ID"):
            file_ids = [os.environ["SMOKE_FILE_ID"]]
        client.send_text({"type": "send_message", "text": message_text, "client_id": client_id, "file_ids": file_ids})
        created = wait_for(
            lambda event: event.get("type") == "message" and event.get("message", {}).get("client_id") == client_id,
            "the sent message event",
        )
        message = created["message"]
        message_id = message["id"]
        if message.get("text") != message_text:
            raise RuntimeError("message payload was not preserved")
        if file_ids and message.get("file_ids") != file_ids:
            raise RuntimeError("message attachment metadata was not preserved")

        client.send_text({"type": "send_message", "text": message_text, "client_id": client_id, "file_ids": file_ids})
        retried = wait_for(
            lambda event: event.get("type") == "message" and event.get("message", {}).get("client_id") == client_id,
            "the idempotent retry event",
        )
        if retried["message"].get("id") != message_id:
            raise RuntimeError("client retry created a duplicate message")

        edit_text = os.environ.get("SMOKE_EDIT_TEXT", "edited websocket smoke message")
        client.send_text({"type": "edit_message", "id": message_id, "text": edit_text})
        edited = wait_for(
            lambda event: event.get("type") == "message_updated" and event.get("message", {}).get("id") == message_id,
            "the edited message event",
        )
        if edited["message"].get("text") != edit_text or not edited["message"].get("edited"):
            raise RuntimeError("message edit was not preserved")
        if file_ids and edited["message"].get("file_ids") != file_ids:
            raise RuntimeError("message attachment metadata was lost during edit")

        client.send_text({"type": "add_reaction", "message_id": message_id, "emoji": "👍"})
        reaction = wait_for(
            lambda event: event.get("type") == "reaction_updated" and event.get("reaction", {}).get("message_id") == message_id,
            "the reaction event",
        )
        if reaction["reaction"].get("emoji") != "👍":
            raise RuntimeError("reaction payload was not preserved")

        client.send_text({"type": "remove_reaction", "message_id": message_id, "emoji": "👍"})
        removed_reaction = wait_for(
            lambda event: event.get("type") == "reaction_updated"
            and event.get("reaction", {}).get("message_id") == message_id
            and event.get("reaction", {}).get("emoji") == "👍",
            "the reaction removal event",
        )
        if removed_reaction["reaction"].get("user_ids"):
            raise RuntimeError("reaction removal was not preserved")

        client.send_text({"type": "typing", "typing": True})
        wait_for(
            lambda event: event.get("type") == "typing"
            and event.get("typing") is True
            and event.get("username") == welcome["username"],
            "the typing-start event",
        )
        client.send_text({"type": "typing", "typing": False})
        wait_for(
            lambda event: event.get("type") == "typing"
            and event.get("typing") is False
            and event.get("username") == welcome["username"],
            "the typing-stop event",
        )

        client.send_text({"type": "load_thread", "message_id": message_id, "before_created_at": None, "before_id": None})
        thread = wait_for(
            lambda event: event.get("type") == "thread_history" and event.get("root_message_id") == message_id,
            "the thread history event",
        )
        if not isinstance(thread.get("messages"), list):
            raise RuntimeError("thread history payload was invalid")

        client.send_text({
            "type": "load_history",
            "channel": message.get("channel", "main"),
            "before_created_at": message["created_at"],
            "before_id": message_id,
        })
        history_page = wait_for(
            lambda event: event.get("type") == "history_page"
            and event.get("channel") == message.get("channel", "main"),
            "the history page event",
        )
        if not isinstance(history_page.get("messages"), list):
            raise RuntimeError("history page payload was invalid")

        client.send_text({"type": "list_channels"})
        channels = wait_for(
            lambda event: event.get("type") == "channels"
            and isinstance(event.get("channels"), list),
            "the channel list event",
        )
        if "main" not in channels["channels"]:
            raise RuntimeError("channel list did not contain main")

        client.send_text({"type": "heartbeat"})
        wait_for(
            lambda event: event.get("type") == "participant_joined"
            and event.get("participant", {}).get("username") == welcome["username"],
            "the heartbeat presence event",
        )

        client.send_text({"type": "mark_read", "message_id": message_id, "created_at": message["created_at"]})
        wait_for(
            lambda event: event.get("type") == "read_state_updated" and event.get("message_id") == message_id,
            "the read-state event",
        )

        if os.environ.get("SMOKE_KEEP_MESSAGE") != "true":
            client.send_text({"type": "delete_message", "id": message_id})
            deleted = wait_for(
                lambda event: event.get("type") == "message_updated" and event.get("message", {}).get("id") == message_id and event.get("message", {}).get("deleted"),
                "the deletion event",
            )
            if not deleted["message"].get("deleted"):
                raise RuntimeError("message deletion was not preserved")
            if file_ids and deleted["message"].get("file_ids") != file_ids:
                raise RuntimeError("message attachment metadata was lost during deletion")

        management_channel = os.environ.get("SMOKE_MANAGEMENT_CHANNEL")
        management_user_id = os.environ.get("SMOKE_MANAGEMENT_USER_ID")
        if management_channel and management_user_id:
            client.send_text({
                "type": "invite_member",
                "channel": management_channel,
                "user_id": management_user_id,
            })
            wait_for(
                lambda event: event.get("type") == "members"
                and event.get("channel") == management_channel
                and any(member.get("user_id") == management_user_id for member in event.get("members", [])),
                "the WebSocket member invitation refresh",
            )
            client.send_text({
                "type": "remove_member",
                "channel": management_channel,
                "user_id": management_user_id,
            })
            wait_for(
                lambda event: event.get("type") == "members"
                and event.get("channel") == management_channel
                and not any(member.get("user_id") == management_user_id for member in event.get("members", [])),
                "the WebSocket member removal refresh",
            )

        create_channel_name = os.environ.get("SMOKE_CREATE_CHANNEL_NAME")
        if create_channel_name:
            client.send_text({"type": "create_channel", "name": create_channel_name})
            wait_for(
                lambda event: event.get("type") == "channel_created"
                and event.get("name") == create_channel_name,
                "the WebSocket channel creation event",
            )

        create_private_name = os.environ.get("SMOKE_CREATE_PRIVATE_CHANNEL_NAME")
        if create_private_name:
            client.send_text({"type": "create_private_channel", "name": create_private_name})
            wait_for(
                lambda event: event.get("type") == "private_conversations"
                and any(item.get("name") == create_private_name for item in event.get("conversations", [])),
                "the WebSocket private conversation creation response",
            )

        delete_channel_name = os.environ.get("SMOKE_DELETE_CHANNEL_NAME")
        if delete_channel_name:
            client.send_text({"type": "delete_channel", "name": delete_channel_name})
            wait_for(
                lambda event: event.get("type") == "channel_deleted"
                and event.get("name") == delete_channel_name,
                "the WebSocket channel deletion event",
            )

        open_direct_user_id = os.environ.get("SMOKE_OPEN_DIRECT_USER_ID")
        if open_direct_user_id:
            client.send_text({"type": "open_direct", "user_id": open_direct_user_id})
            wait_for(
                lambda event: event.get("type") == "private_conversations"
                and any(item.get("peer_user_id") == open_direct_user_id for item in event.get("conversations", [])),
                "the WebSocket direct conversation response",
            )
        print(message_id)
        return 0
    finally:
        client.close()


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (OSError, RuntimeError, ValueError, json.JSONDecodeError) as error:
        print(f"websocket smoke failed: {error}", file=sys.stderr)
        raise SystemExit(1)
