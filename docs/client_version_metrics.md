# Client verison metrics

Currently, `sqld` supports clients passing their client version via a
`x-libsql-client-version` header. The value of this header should follow this
pattern:

- Hrana/Remote clients should be `libsql-remote-<language>-<version>`
- Embedded replica clients should be `libsql-rpc-<language>-<version>`


`<language>` should be a reference to the language, for example,
`rust`/`go`/`js`/`python`.

`<version>` should be a reference to either a semver version or a commit sha
(first 6 chars of the sha).
