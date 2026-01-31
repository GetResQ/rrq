from __future__ import annotations

import struct

from rrq.protocol import decode_message, encode_message


def test_encode_decode_message_roundtrip() -> None:
    payload = {"job_id": "job-1", "status": "success"}
    frame = encode_message("response", payload)
    length = struct.unpack(">I", frame[:4])[0]
    assert length == len(frame) - 4
    message_type, decoded_payload = decode_message(frame[4:])
    assert message_type == "response"
    assert decoded_payload == payload
