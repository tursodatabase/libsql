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

exp = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=3)
claims = {
    "exp": int(exp.timestamp()),
}
token = jwt.encode(claims, privkey_pem, "EdDSA")

claims["a"] = "ro"
ro_token = jwt.encode(claims, privkey_pem, "EdDSA")

open("jwt_key.pem", "wb").write(pubkey_pem)
open("jwt_key.base64", "wb").write(pubkey_base64)
print(f"Full access: {token}")
print(f"Read-only:   {ro_token}")
