# Aerospike Java SDK Examples

The examples module contains runnable examples that double as documentation source. Example classes should focus on the SDK usage being demonstrated; the runner owns connection setup, configuration, verification, cleanup, and CI reporting.

## Run Examples

Build the examples JAR:

```bash
mvn -pl examples -am package -DskipTests
```

Run one example:

```bash
./examples/run_examples BatchExample -h localhost -p 3000 -n test
```

Run all registered examples:

```bash
./examples/run_examples all -h localhost -p 3000 -n test
```

Write a JUnit XML report:

```bash
./examples/run_examples all \
  -h localhost \
  -p 3000 \
  -n test \
  --report examples/target/example-reports/TEST-examples.xml
```

Useful runner options:

- `--fail-fast`: stop after the first failed example.
- `--include-tags smoke,records`: run examples with at least one matching tag.
- `--exclude-tags config`: skip examples with matching tags.

## Add An Example

1. Create a class in `src/main/java/com/aerospike/examples` that extends `Example`.
2. Implement `runExample()` with no constructor or runner arguments; use `cluster()`, `dataSet()`, `namespace()`, and `console` from `Example` when needed.
3. Use `dataSet()` or `dataSet("set-name")` when the example writes records, unless the example is specifically demonstrating a fixed namespace or set.
4. Register the example in `ExampleRegistry` with tags and an `ExampleFixture`.
5. Add verification in a fixture under `com.aerospike.examples.fixtures`.

Every example registered for CI should have an explicit fixture decision:

- Use a fixture with `setup`, `verify`, and `cleanup` when the example changes server state.
- Use `ExampleFixture.NONE` only when there is no meaningful post-run state to verify.
- Throw `ExampleSkipException` for server capability or configuration gates that should be reported as skipped rather than failed.

## Verification Model

The runner executes each registered example in this order:

1. Fixture setup
2. Example body
3. Fixture verification
4. Fixture cleanup
5. Result reporting

Verification should check state after the example runs. Prefer reusable helpers in `ExampleAssertions` for common checks such as truncating a dataset, counting records, checking that a record exists, and comparing bin values.

## CI

PR CI builds the examples module, runs `all` against the Aerospike server provisioned for integration tests, and writes `examples/target/example-reports/TEST-examples.xml`. The report is uploaded with the other test artifacts so example failures are visible as test failures.
