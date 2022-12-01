
[![License](https://img.shields.io/badge/license-MIT-blue)](https://github.com/libsql/libsql/blob/master/LICENSE.md)
[![Discord](https://img.shields.io/discord/1026540227218640906?color=5865F2&label=discord&logo=discord&logoColor=8a9095)](https://discord.gg/TxwbQTWHSr)


# What is libSQL?

[libSQL](https://libsql.org) is an open source, open contribution fork of SQLite. We aim to evolve it to suit many more use cases than SQLite was originally designed for.

## We like SQLite a lot, and with modifications.

Wildly successful, and broadly useful, SQLite has solidified its place in modern technology stacks, embedded in nearly any computing device you can think of. Its open source nature and public domain availability make it a popular choice for modification to meet specific use cases.

## Hack SQLite internally or externally?
It seems to us that there are two obvious avenues to modify SQLite: forking the code to add features directly to it, or running it on top of a modified OS. History suggests that neither of these work well. The way we see it, this is a result of one major limitation of the software: SQLite is open source but does not accept contributions, so community improvements cannot be widely enjoyed.

## Quickstart

```
./configure && make
./libsql <path-to-database.db>
```

## SQLite needs to open contributions.
We want to see a world where everyone can benefit from all of the great ideas and hard work that the SQLite community contributes back to the codebase.  Community contributions work well, because [we’ve done it before][qemu-sqlite]. If this was possible, what do you think SQLite could become?

## Could SQLite become a distributed database?

SQLite is gaining ground in edge use cases, since it is fast, embeddable, and matches well the read-mostly, low-to-medium volume of data use cases that often arise at the edge. But there is still the problem of how to make the data available in all nodes.

That is challenging to do without support from the core database. Without proper hooks, existing solutions either build a different database around SQLite (like dqlite, rqlite, ChiselStore), or have to replicate at the filesystem layer (LiteFS).

What if we could do it natively?

## Could SQLite be optimized with an asynchronous API?

Recently, Linux has gained a new supposedly magical interface called [`io_uring`](https://www.theregister.com/2022/09/16/column/). It leverages Asynchronous I/O and has been slowly but surely gaining adoption everywhere. Other databases like Postgres have already adopted it for asynchronous I/O, but this hasn’t made its way to SQLite. Supposedly because SQLite interfaces are synchronous. But with asynchronous runtimes and interfaces gaining popularity in both languages and the Kernel, is it time to add a new, async interface to SQLite, that plays well with `io_uring` ?

## Could SQLite be embedded in the Linux kernel?

Another innovation in the Linux Kernel is [eBPF](https://www.scylladb.com/2020/05/05/how-io_uring-and-ebpf-will-revolutionize-programming-in-linux/). That is a domain-specific VM that allows programs to execute in the kernel. Although still mainly used for tracing, there’s [research](https://www.asafcidon.com/uploads/5/9/7/0/59701649/xrp.pdf) about pushing complex data functions like B-Tree lookups entirely in the kernel, as close as possible to NVMe devices. What if SQLite could take advantage of that, and also be unbeatable in workloads that don’t fit in memory?
 
## Could SQLite support WASM user-defined functions?

SQLite does support [user-defined functions](http://www.sqlite.org/c3ref/create_function.html). But there are two big problems with how they are approached: first, functions are written in C, which is increasingly becoming a tall ask for most developers with safety in mind. WASM is growing in popularity, allowing developers to write functions in their preferred language and be safely executed.

Second, once a function proves itself generally useful, the “not Open Contribution” policy makes it difficult for it to be included in the standard distribution of SQLite.

## Could you be a part of the team that makes these possible?
We’ve decided that now is the time to take SQLite to the places where the community wants to be.  Are you interested in building the future of SQLite? We’re kicking off our efforts on GitHub, and you can find us on Discord.


# Our plan

## Start with a fork of SQLite
There is nothing to be gained in reinventing greatness, so we will simply build upon that with a fork of core SQLite.  Unlike SQLite, this fork will be both fully open source (MIT) and open to community contributions.

## Preserve compatibility
We are committed to preserving compatibility with everything that was previously written for SQLite.  All of your favorite tools and libraries will continue to work as-is.

## Preserve stability
SQLite is a very well-tested piece of software.  We admire that, and commit to preserving the existing test suite while expanding it for the new code we add.

## Use Rust for new features
We intend to add Rust to implement new capabilities, but not exclusively so.  The existing C codebase already serves as a great foundation for interoperability with other languages and systems, and we don’t intend to change that.

## Rejoin core SQLite if its policy changes
We are strong believers in open source that is also open to community contributions. If and when SQLite changes its policy to accept contributions, we will gladly merge our work back into the core product and continue in that space.

## Adhere to a code of conduct
We take our code of conduct seriously, and unlike SQLite, we do not substitute it with an [unclear alternative](https://sqlite.org/codeofethics.html).  We strive to foster a community that values diversity, equity, and inclusion.  We encourage others to speak up if they feel uncomfortable.

[qemu-sqlite]: https://glaubercosta-11125.medium.com/sqlite-qemu-all-over-again-aedad19c9a1c
