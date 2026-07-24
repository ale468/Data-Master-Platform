# Governance

Data Master Platform is maintained through public code, tests, documentation,
issues and pull requests.

## Authority

- `main` is the current source for code, tests, infrastructure and public
  documentation.
- A merged change does not by itself prove runtime behavior.
- Reproducible public validation is required before promoting a technical
  claim.
- Roadmap items remain future work until implementation and evidence exist.

## Decision making

Maintainers review changes for scope, correctness, security, reproducibility,
licensing and claim accuracy. Material changes should explain the chosen
trade-off in the pull request or a public design record.

## Release boundary

The initial repository history intentionally starts with one clean root commit.
Private development history, prompts and evidence are not part of this public
artifact.

Container publication, releases and visibility changes require a separate
maintainer decision. CI builds validation images with publishing disabled.

## Third-party material

Dependencies and images keep their own licenses and notices. Names and
trademarks such as Apache, Airflow, Spark, Kubernetes, MinIO, PostgreSQL and
others belong to their respective owners. Their appearance does not imply
endorsement.

Some project material was developed with AI-assisted tools and reviewed by the
maintainer. Private prompts and conversations are not required to build, run or
modify this repository.
