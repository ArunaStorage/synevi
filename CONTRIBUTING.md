# CONTRIBUTING

Thank you for your interest in contributing to the project. Issues, Bug reports or feature request can be made via GitHub issues. For detailed developer information please see the sections below.

In any case please also acknowledge our [Code of Conduct](CODE_OF_CONDUCT.md)


## Developer Contributions Guidance

Please make sure that all contributions compile and do not produce any errors. For this please run:

```
cargo +nightly clippy
```

and

```
cargo +nightly fmt
```

to check if your contributions follow the styling and syntax recommendations.

### Testing

All commits should be tested before a PR is issued. See our [Testing Guidelines](./tests/README.md) to learn more how to test synevi.