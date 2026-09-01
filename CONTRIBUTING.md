# Contributing

Thanks for helping improve Data Master Platform.

## Workflow

1. Open or select a public issue with a narrow outcome.
2. Create a short-lived branch from `main`.
3. Keep code, infrastructure and documentation changes focused.
4. Run the tests and builds directly related to the change.
5. Open a pull request describing impact, validation and limitations.

Useful checks:

```powershell
python -m compileall -q dags jobs tests
python -m unittest discover -s tests\runtime -p "test_*.py"
git diff --check
```

Image changes must build without publishing:

```powershell
docker build --file Dockerfile.airflow --tag data-master-airflow:validation .
docker build --file Dockerfile.spark --tag data-master-spark:validation .
```

## Boundaries

Do not submit:

- real customer or personal data;
- secrets, credentials or private keys;
- private prompts, internal instructions or private repository references;
- proprietary challenge documents or third-party material without permission;
- claims of production, cloud or regulatory readiness without reproducible
  public evidence.

Keep synthetic fixtures deterministic. Preserve masking and the local controlled
demo path.

## Licensing

By submitting a contribution, you confirm that you have the right to provide it
and agree that it is licensed under `AGPL-3.0-only`. Third-party material must
retain its original license and attribution.

## Provenance of new files

Before committing a new source file, classify it in
`config/provenance/copyright-policy.json`. Add the two-line project header only
when the file is an original project contribution. Preserve upstream notices
and place generated, scaffolded or third-party material in a documented
exception instead of modifying it automatically.

Run `python scripts/validate_provenance.py` before opening a pull request. The
optional local hook can be enabled with `pre-commit install`.

All contributors must follow [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
