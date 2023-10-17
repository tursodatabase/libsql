#!/usr/bin/env python3
"""utility that generates X.509 certificates for testing

the following certificates and their keys are stored in your working directory:
- ca_cert.pem, ca_key.pem
- server_cert.pem, server_key.pem
- client_cert.pem, client_key.pem
"""
import datetime
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

def gen_key():
    return Ed25519PrivateKey.generate()

not_before = datetime.datetime.now(datetime.timezone.utc)
not_after = not_before + datetime.timedelta(days=3)

def gen_ca_cert(ca_key):
    ca_name = x509.Name([
        x509.NameAttribute(x509.oid.NameOID.COMMON_NAME, "sqld dev CA"),
    ])
    return x509.CertificateBuilder() \
        .issuer_name(ca_name) \
        .subject_name(ca_name) \
        .public_key(ca_key.public_key()) \
        .serial_number(x509.random_serial_number()) \
        .not_valid_before(not_before) \
        .not_valid_after(not_after) \
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True) \
        .add_extension(x509.KeyUsage(
            key_cert_sign=True,
            crl_sign=True,
            digital_signature=False,
            content_commitment=False,
            key_encipherment=False,
            data_encipherment=False,
            key_agreement=False,
            encipher_only=False,
            decipher_only=False,
        ), critical=True) \
        .sign(ca_key, None)

def gen_peer_cert(ca_cert, ca_key, peer_key, peer_common_name, peer_dns_names):
    return x509.CertificateBuilder() \
        .issuer_name(ca_cert.subject) \
        .subject_name(x509.Name([
            x509.NameAttribute(x509.oid.NameOID.COMMON_NAME, peer_common_name),
        ])) \
        .public_key(peer_key.public_key()) \
        .serial_number(x509.random_serial_number()) \
        .not_valid_before(not_before) \
        .not_valid_after(not_after) \
        .add_extension(x509.BasicConstraints(ca=False, path_length=None), critical=True) \
        .add_extension(x509.KeyUsage(
            digital_signature=True,
            key_encipherment=False,
            key_cert_sign=False,
            crl_sign=False,
            content_commitment=False,
            data_encipherment=False,
            key_agreement=False,
            encipher_only=False,
            decipher_only=False,
        ), critical=True) \
        .add_extension(x509.SubjectAlternativeName([
            x509.DNSName(dns_name) for dns_name in peer_dns_names
        ]), critical=False) \
        .sign(ca_key, None)

def store_cert_chain_and_key(cert_chain, key, name) -> None:
    cert_file = f"{name}_cert.pem"
    key_file = f"{name}_key.pem"

    with open(cert_file, "wb") as f:
        for cert in cert_chain:
            f.write(cert.public_bytes(encoding=serialization.Encoding.PEM))
    print(f"stored cert {name!r} into {cert_file!r}")

    with open(key_file, "wb") as f:
        f.write(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ))
    print(f"stored private key {name!r} into {key_file!r}")

if __name__ == "__main__":
    ca_key = gen_key()
    ca_cert = gen_ca_cert(ca_key)
    store_cert_chain_and_key([ca_cert], ca_key, "ca")

    server_key = gen_key()
    server_cert = gen_peer_cert(ca_cert, ca_key, server_key, "sqld", ["sqld"])
    store_cert_chain_and_key([server_cert, ca_cert], server_key, "server")

    client_key = gen_key()
    client_cert = gen_peer_cert(ca_cert, ca_key, client_key, "sqld replica", [])
    store_cert_chain_and_key([client_cert, ca_cert], client_key, "client")

    print(f"these are development certs, they will expire at {not_after}")
