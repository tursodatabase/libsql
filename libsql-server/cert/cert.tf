resource "tls_self_signed_cert" "root" {
  private_key_pem = file("../ca.key")

  subject {
    common_name  = "example.com"
    organization = "ACME Examples, Inc"
  }

  validity_period_hours = 12

  allowed_uses = ["cert_signing"]
}

resource "local_file" "ca_cert" {
  filename = "../ca.pem"
  content = tls_self_signed_cert.root.cert_pem
}

resource "tls_cert_request" "server" {
  private_key_pem = file("../server1.key")

  subject {
    common_name  = "example.com"
    organization = "ACME Examples, Inc"
  }
}

resource "tls_locally_signed_cert" "server" {
  cert_request_pem   = tls_cert_request.server.cert_request_pem
  ca_private_key_pem = tls_self_signed_cert.root.private_key_pem
  ca_cert_pem        = tls_self_signed_cert.root.cert_pem

  validity_period_hours = 12

  allowed_uses = [
    "key_encipherment",
    "digital_signature",
    "server_auth",
  ]
}

resource "local_file" "server_cert" {
  filename = "../server1.pem"
  content = tls_locally_signed_cert.server.cert_pem
}

