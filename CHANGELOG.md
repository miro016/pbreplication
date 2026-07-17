# Changelog

## [0.4.1](https://github.com/miro016/pbreplication/compare/v0.4.0...v0.4.1) (2026-07-17)


### Bug Fixes

* detect and heal duplicate node ids from cloned data directories ([3a017b8](https://github.com/miro016/pbreplication/commit/3a017b83d7e9677832b9bcc9b019e41382702793))
* detect and heal duplicate node ids from cloned data directories ([4d94566](https://github.com/miro016/pbreplication/commit/4d9456685645fddb1aea17ce109d6ec644b63214))

## [0.4.0](https://github.com/miro016/pbreplication/compare/v0.3.0...v0.4.0) (2026-07-12)


### Features

* cluster-coordinated migrations on every startup ([f591967](https://github.com/miro016/pbreplication/commit/f591967a36c757747aaa87c3ebddae2eb431f0b8))
* dashboard sync progress, lag column, events tab; docs for the new stack ([b2eb7f3](https://github.com/miro016/pbreplication/commit/b2eb7f35d97f4493a43a81c29ba079bb2a6a602c))
* exported Go status API (Status, SyncStatus, Counters, PeerLags) ([ec9c2d7](https://github.com/miro016/pbreplication/commit/ec9c2d7e135c85a69ffe019a8e5df70fca8a7b2d))
* full database copy bootstrap with offline-write rescue ([504d13e](https://github.com/miro016/pbreplication/commit/504d13ea753a52599bca96122e5abff2dfa2b74f))
* harden node-to-node transport (per-request timeouts, retries, resumable streams) ([a664d65](https://github.com/miro016/pbreplication/commit/a664d658d6f3e142379693c2e262e7649debaae4))
* harden node-to-node transport (per-request timeouts, retries, resumable streams) ([c197d6f](https://github.com/miro016/pbreplication/commit/c197d6f073e4f993aca79cc5883d630e2498a826))
* relation-integrity validation after bulk syncs ([23c9811](https://github.com/miro016/pbreplication/commit/23c9811fd2481103cdb9f51d707a9001ae729269))
* replication event timeline, health-transition logs and per-peer lag ([64e7002](https://github.com/miro016/pbreplication/commit/64e70025b372ecb6d7b25c8366c57a0fe9f25699))


### Performance

* batched snapshot applies, persisted resume cursor, memory-safe reconcile ([7bf2460](https://github.com/miro016/pbreplication/commit/7bf246063b68a5c62596c8fd2fb52eb24b710bc9))

## [0.3.0](https://github.com/miro016/pbreplication/compare/v0.2.0...v0.3.0) (2026-07-09)


### Features

* expose Members, PeerURLs and leader accessors ([dcb6c4c](https://github.com/miro016/pbreplication/commit/dcb6c4c8825bb3b44ab51b00f41f2ac938b2c8d0))
* expose Members, PeerURLs and leader accessors ([de63758](https://github.com/miro016/pbreplication/commit/de63758fab2a78980da22e0a8385064cf9c4ce6b))

## [0.2.0](https://github.com/miro016/pbreplication/compare/v0.1.0...v0.2.0) (2026-07-09)


### Features

* run app migrations after the initial full sync on joining nodes ([c70f97b](https://github.com/miro016/pbreplication/commit/c70f97b816a726f7aea7273c7eaa1686366983c7))

## 0.1.0 (2026-07-03)


### CI

* add release-please for automated versioning ([4e7d990](https://github.com/miro016/pbreplication/commit/4e7d99057847992581c0bca9ffc60607e403eaa7))
