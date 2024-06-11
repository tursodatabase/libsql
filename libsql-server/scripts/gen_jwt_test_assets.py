#!/usr/bin/env python3
"""utility that generates Ed25519 key and a JWT for testing

the public key is stored in jwt_key.pem (in PEM format) and jwt_key.base64 (raw
base64 format) and the JWT is printed to stdout
"""
import base64
import datetime
import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

def update_example(name, namespaces):
    privkey = Ed25519PrivateKey.generate()
    pubkey = privkey.public_key()

    pubkey_pem = pubkey.public_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PublicFormat.SubjectPublicKeyInfo,
    )

    pubkey_base64 = base64.b64encode(
        pubkey.public_bytes(
            encoding=serialization.Encoding.Raw,
            format=serialization.PublicFormat.Raw,
        ),
        altchars=b"-_",
    )
    while pubkey_base64[-1] == ord("="):
        pubkey_base64 = pubkey_base64[:-1]

    privkey_pem = privkey.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    exp = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=100_000)
    claims = {
        "p": { "ro": { "ns": namespaces } },
        "exp": int(exp.timestamp()),
    }
    token = jwt.encode(claims, privkey_pem, "EdDSA")
    open(f"libsql-server/assets/test/auth/{name}.key", "wb").write(privkey_pem)
    open(f"libsql-server/assets/test/auth/{name}.pem", "wb").write(pubkey_pem)
    open(f"libsql-server/assets/test/auth/{name}.jwt", "wb").write(token.encode())
    open(f"libsql-server/assets/test/auth/combined123.pem", "ab").write(pubkey_pem)

open(f"libsql-server/assets/test/auth/combined123.pem", "wb").write("".encode())
update_example("example1", ["example1a", "example1b", "example1c"])
update_example("example2", ["example2d"])
update_example("example3", ["example3e", "example3f"])
