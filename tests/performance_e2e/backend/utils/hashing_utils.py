import base64
import hashlib
import json


def hash_to_string(*xs, length) -> str:
    s = json.dumps(xs, sort_keys=True).encode()
    # 16-byte deterministic hash -> hex string of length 32
    h = hashlib.blake2b(s, digest_size=16).digest()
    b64 = base64.urlsafe_b64encode(h).decode("ascii")
    return (b64 * ((length // len(b64)) + 1))[:length]


def hash_to_uniform_64bit(*xs) -> float:
    s = json.dumps(xs, sort_keys=True).encode()
    # 8-byte deterministic hash -> integer in [0, 2^64-1]
    h = hashlib.blake2b(s, digest_size=8).digest()
    return int.from_bytes(h, "big")


def hash_to_uniform_0_1(*xs) -> float:
    return hash_to_uniform_64bit(*xs) / 2**64
