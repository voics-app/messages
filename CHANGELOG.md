# Changelog

All notable changes to this project are documented here. The file is maintained by
release-please from Conventional Commits.

## 0.1.0

First release: everything on develop up to this point, deployed to gen-serv.

## 0.1.0

- Initial implementation of the messages service: ScyllaDB bucketed history, reactions, pins,
  read states, attachments through MinIO, event publishing to `discordy.message.v1`, and the
  internal last-message-id and admin stats endpoints.
