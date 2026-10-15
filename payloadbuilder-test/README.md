# payloadbuilder-test

JUnit 5 utilities for testing PayloadBuilder SQL queries, including a **query coverage extension** that shows which operators, filters, joins and CASE/IF branches were exercised by your tests.

---

## Dependency

```xml
<dependency>
    <groupId>se.kuseman.payloadbuilder</groupId>
    <artifactId>payloadbuilder-test</artifactId>
    <version>${payloadbuilder.version}</version>
    <scope>test</scope>
</dependency>
```

---

## Query Coverage

### What it tracks

| Item | What "covered" means |
|------|----------------------|
| **Filter** (WHERE / HAVING) | Executed at least once with at least one matching row |
| **Join** (INNER / LEFT / CROSS APPLY etc.) | Executed at least once |
| **Projection** | Executed at least once |
| **TableScan / TableFunctionScan** | Executed at least once |
| **CASE WHEN branch** | That specific WHEN clause matched at least one row |
| **ELSE branch** | At least one row fell through to ELSE |
| **AND / OR condition** | Both the `true` and `false` outcomes were observed at least once |

Source locations (line numbers) are attached to operators and expressions when available, so you can pinpoint exactly which WHERE clause, JOIN, or AND branch was missed.

### The `__coverage__` column

Every result set produced while coverage is active gets an extra `__coverage__` column appended as the last column. The value is identical on every row — it is a JSON snapshot of the plan coverage for that query:

```json
{
  "operators": {
    "3": { "name": "Filter", "execution_count": 1, "rows_out": 3, "covered": true,  "location": { "line": 2, "start": 42, "end": 57 } },
    "5": { "name": "HashMatch", "execution_count": 0, "rows_out": 0, "covered": false }
  },
  "branches": {
    "7": { "expression": "CASE WHEN x > 0 ...", "when_hits": [4, 0], "else_hits": 2, "location": { "line": 3, "start": 10, "end": 60 } }
  },
  "conditions": {
    "9": { "expression": "x > 1 AND x < 10", "true_hits": 3, "false_hits": 1, "location": { "line": 2, "start": 62, "end": 78 } }
  }
}
```

> **Note** — if your existing tests assert on exact column count or schema, they will need updating to account for the extra column.

---

## Usage

### Option A — `@ExtendWith` (console summary only)

Simplest form. Gives a summary in the JUnit console output and writes a JSON report to `target/coverage/`.

```java
@ExtendWith(QueryCoverageExtension.class)
class MyQueryTest {

    @Test
    void myTest() {
        // run queries, make normal assertions
    }
}
```

Console output after all tests:

```
TestIdentifier [MyQueryTest]
ReportEntry [query-coverage = 'operators: 18/24 covered, branches: 3/4 covered']
```

---

### Option B — `@RegisterExtension` + `@TestFactory` (per-operator JUnit entries)

Gives individual pass/fail entries in the JUnit view — one per operator and one per CASE/IF branch. Uncovered items show as red failures, covered items as green passes.

`QueryCoverageExtension` has no JUnit `DynamicTest` dependency itself — the dynamic test generation lives in `CoverageTestFactory` so the extension loads cleanly as a `@RegisterExtension` field.

```java
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)  // required — factory must run last
class MyQueryTest {

    @RegisterExtension
    static final QueryCoverageExtension coverage = new QueryCoverageExtension();

    @Test
    @Order(1)
    void someQuery() {
        // ...
    }

    @ParameterizedTest
    @Order(2)
    @CsvSource({ "..." })
    void parameterisedQuery(String arg) {
        // ...
    }

    // Must be last so the accumulator is fully populated
    @TestFactory
    @Order(Integer.MAX_VALUE)
    Stream<DynamicNode> queryCoverageResults() {
        return CoverageTestFactory.from(coverage.getAccumulator());
    }
}
```

JUnit view result:

```
▼ queryCoverageResults()
  ▼ query #1
    ✓ Projection [id:0]
    ✗ Filter [line 5]          ← red: "not covered — 0 executions"
    ✓ HashMatch [line 3]
  ▼ query #2
    ✓ CASE [line 8] → WHEN 0 (4 hits)
    ✗ CASE [line 8] → WHEN 1 (0 hits)   ← red: "WHEN 1 never matched — 0 hits"
    ✓ CASE [line 8] → ELSE (2 hits)
```

---

## Sharing test cases with a Spring Boot integration test

If you already have a `@SpringBootTest` class testing the same queries via HTTP, extract the shared test cases into an abstract base class and have both test classes extend it:

```java
// Base — no framework annotations, just the test data
abstract class AbstractProductQueryTest {

    @ParameterizedTest
    @CsvSource(textBlock = """
        /v1/products/123; 200; /expected/product_123.json
        /v1/products/999; 404; <null>
    """, nullValues = "<null>", delimiter = ';')
    void testProductEndpoint(String path, int expectedStatus, String expectedJson) throws IOException {
        assertResponse(path, expectedStatus, expectedJson);
    }

    protected abstract void assertResponse(String path, int expectedStatus, String expectedJson) throws IOException;
}
```

```java
// Spring Boot integration test (HTTP)
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
class ProductControllerIT extends AbstractProductQueryTest {

    @Autowired TestRestTemplate restTemplate;

    @Override
    protected void assertResponse(String path, int expectedStatus, String expectedJson) {
        // assert via HTTP
    }
}
```

```java
// Coverage test (direct service call, no Spring context)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ProductQueryCoverageTest extends AbstractProductQueryTest {

    @RegisterExtension
    static QueryCoverageExtension coverage = new QueryCoverageExtension();

    @Override
    protected void assertResponse(String path, int expectedStatus, String expectedJson) {
        // call service directly — same assertions, but queries run on the test thread
        // so the thread-local coverage registry picks them up
    }

    @TestFactory
    @Order(Integer.MAX_VALUE)
    Stream<DynamicNode> queryCoverageResults() {
        return coverage.buildCoverageTests();
    }
}
```

> **Why not use `@SpringBootTest` for coverage?**
> `@SpringBootTest` with `WebEnvironment.RANDOM_PORT` handles requests on Tomcat threads, not the test thread.
> `QueryCoverageExtension` uses a `ThreadLocal`, so it only sees queries executed on the thread that registered the listener.
> Direct service calls stay on the test thread and work correctly.

---

## JSON report

After each test class run a file is written to:

```
target/coverage/<TestClassName>-coverage.json
```

Format:

```json
{
  "test_class": "ProductQueryCoverageTest",
  "queries": [
    {
      "operators": { ... },
      "branches":  { ... }
    }
  ]
}
```

---

## Annotated SQL files (gutter markers)

When a named query is compiled with `Payloadbuilder.compile(session, sql, "queryName")` and you register the corresponding source file, the coverage report generates an annotated `.annotated.sql` file alongside the JSON report. Each SQL line is prefixed with a gutter character:

| Marker | Meaning |
|--------|---------|
| `✓` | Fully covered — every operator on this line executed and every CASE/IF branch and AND/OR outcome was seen |
| `~` | Partially covered — the line was reached but at least one branch or outcome was never hit |
| `✗` | Present in the plan but never executed |
| ` ` | No coverage data for this line (comments, blank lines, DDL) |

Register the source file on the extension:

```java
@RegisterExtension
static final QueryCoverageExtension coverage = new QueryCoverageExtension()
        .registerSource("my-query", "src/test/resources/queries/my-query.sql");
```

The name must match the third argument to `Payloadbuilder.compile(session, sql, "my-query")`. Multi-statement files compiled as `my-query`, `my-query#1`, … are automatically merged into one annotated file.

Example annotated output written to `target/coverage/MyTest-my-query.annotated.sql`:

```
✓  1 | select *
✓  2 | from orders o
✓  3 | inner join customers c on c.id = o.customerId
~  4 | where o.status = 'open'
✗  5 |   and o.amount > 1000
   6 | order by o.createdAt desc
```

> **Tip** — wire this up in CI by archiving `target/coverage/` as a build artefact. Developers can open the `.annotated.sql` files directly in their IDE to see which lines their test suite never reached.

---

## See also

- `QueryCoverageExtensionExampleTest` in `src/test/java` — runnable end-to-end example including `registerSource` usage
- `CoveragePlan`, `InstrumentedCaseExpression`, `InstrumentedLogicalBinaryExpression` in `payloadbuilder-core` — engine-side implementation
