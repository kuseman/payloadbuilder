# Plan: split payloadbuilder-catalog into one artifact per catalog

Status: plan, 2026-11-19, for payloadbuilder 1.15.0. All decisions are taken (§5).

## 1. Goal

Today `payloadbuilder-catalog` is one jar with every bundled catalog. Anyone who wants one catalog gets all of them, and all of their libraries: `kafka-clients`, `mongodb-driver-sync`, `httpclient5`, HikariCP, mustache.

After the split each catalog is its own artifact, with only its own dependencies:
- `payloadbuilder-catalog-es`
- `payloadbuilder-catalog-fs`
- `payloadbuilder-catalog-http`
- `payloadbuilder-catalog-jdbc`
- `payloadbuilder-catalog-kafka`
- `payloadbuilder-catalog-mongodb`

The combined `payloadbuilder-catalog` artifact is no longer published.

**Why now:** Queryeer Desktop is moving each catalog into the plugin that owns its kind of connection (the MongoDB plugin, a Kafka plugin, the JDBC engine). Each of those plugins should bundle its own catalog and nothing else. The MongoDB plugin, for example, already ships its own `mongodb-driver-sync`. See `queryeer-desktop-plugins/specs/engine-catalogs.md`.

**What doesn't change:**
- Package and class names. A consumer changes Maven dependencies, not imports.
- Behaviour. This is a packaging change only.

## 2. Today (1.15.0-SNAPSHOT)

One module, `payloadbuilder-catalog`. `payloadbuilder-api` is `provided`. Main code uses nothing from `payloadbuilder-core`.

| Package | Classes from the root package | Libraries used |
|---|---|---|
| `es` | `AuthType`, `HttpClientUtils` | httpclient5, jackson-databind, mustache, commons-io, commons-lang3, commons-collections4, slf4j |
| `fs` | | commons-io, commons-lang3 |
| `http` | | httpclient5, jackson-databind, mustache, commons-io, commons-lang3 |
| `jdbc` (with `jdbc/dialect`) | `CredentialsException` | HikariCP, commons-io, commons-lang3, commons-collections4, slf4j |
| `kafka` | `HttpClientUtils` | kafka-clients, avro (optional), httpclient5, jackson-databind, commons-io |
| `mongodb` | `CredentialsException` | mongodb-driver-sync, commons-lang3, slf4j |

- No catalog package refers to another catalog package, in main or test code.
- **The root package `se.kuseman.payloadbuilder.catalog` holds three classes:**
  - `CredentialsException`, used by `jdbc` and `mongodb`;
  - `AuthType`, used by `HttpClientUtils` and `es`;
  - `HttpClientUtils` (httpclient5 and commons-lang3), used by `es` and `kafka`.
- **Tests:** a shared `TestUtils` in the root test package, used by every package's tests. Test-only dependencies:
  - testcontainers (Kafka, MongoDB, Elasticsearch, JDBC);
  - mockserver (es, http);
  - the JDBC drivers (mssql, mariadb, mysql, postgresql, oracle);
  - `slf4j-reload4j`;
  - `payloadbuilder-core`, and `payloadbuilder-api`'s test-jar.

**Consumers known today:**
- Queryeer Desktop's `backend-plugin-payloadbuilder`;
- the Swing Queryeer's `queryeer-catalog` module (`../queryeer`, and `../queryeer_electron`);
- the README's "Optionally add bundled catalogs" snippet.

## 3. Design

### 3.1 Modules

`payloadbuilder-catalog/` becomes a parent POM (packaging `pom`, artifact `payloadbuilder-catalog-parent`) with one module per artifact:

```text
payloadbuilder-catalog/
  pom.xml                    payloadbuilder-catalog-parent: shared plugin config, dependencyManagement for catalog libs
  common/                    payloadbuilder-catalog-common
  es/  fs/  http/  jdbc/  kafka/  mongodb/
```

- The root `pom.xml` keeps `<module>payloadbuilder-catalog</module>`.
- Each module's artifactId is `payloadbuilder-catalog-<name>`, and its source moves as it is (`git mv`), packages unchanged:
  - `src/main/java/se/kuseman/payloadbuilder/catalog/<name>/**` goes to `<name>/src/main/java/se/kuseman/payloadbuilder/catalog/<name>/**`;
  - the same for tests and test resources.
- The library versions now declared in the catalog POM's properties move to the parent's `dependencyManagement`: `kafka-clients.version`, `mongodb-driver.version`, `avro.version`, `testcontainers-version`, HikariCP, mustache and httpclient5. The modules declare dependencies without versions.

### 3.2 The root package: `payloadbuilder-catalog-common`

All three root classes go into one module, `payloadbuilder-catalog-common`: `CredentialsException`, `AuthType` and `HttpClientUtils`.
- **httpclient5 is `<optional>true</optional>` in common.** `es` and `kafka` declare httpclient5 themselves, since they use it directly anyway. `jdbc` and `mongodb` get common without httpclient5. They never load `HttpClientUtils`, so the class's presence costs nothing.
- **Why one module rather than two** (for example a separate `payloadbuilder-catalog-httpclient`): two jars with classes in the same package `se.kuseman.payloadbuilder.catalog` would be a split package. That is harmless on the class path, but it breaks as soon as anyone uses JPMS modules. Moving the classes to new packages would avoid it, but would break imports for consumers. One module avoids both.
- **Test code:** common also publishes a test-jar with `TestUtils`. Each catalog module's tests depend on it (`<type>test-jar</type>`, `test` scope).

### 3.3 Dependencies per artifact

`payloadbuilder-api` stays `provided` everywhere, and commons-io and commons-lang3 come from the root `dependencyManagement` as today.

| Artifact | Compile dependencies | Test dependencies (beyond common's test-jar, junit, mockito, `payloadbuilder-core`, `payloadbuilder-api` test-jar, slf4j-reload4j) |
|---|---|---|
| `-common` | commons-lang3, httpclient5 (optional) | |
| `-es` | common, httpclient5, jackson-databind, mustache, commons-io, commons-lang3, commons-collections4, slf4j-api | testcontainers, mockserver |
| `-fs` | commons-io, commons-lang3 | |
| `-http` | httpclient5, jackson-databind, mustache, commons-io, commons-lang3 | mockserver |
| `-jdbc` | common, HikariCP (excluding slf4j-api, as today), commons-io, commons-lang3, commons-collections4, slf4j-api | testcontainers, the JDBC drivers |
| `-kafka` | common, kafka-clients, avro (optional, as today), httpclient5, jackson-databind, commons-io | testcontainers |
| `-mongodb` | common, mongodb-driver-sync, commons-lang3, slf4j-api | testcontainers |

- **Check each row** against `mvn dependency:analyze` once the modules compile. The table comes from reading the imports, and something may be used only through a transitive dependency.
- **Client libraries stay at compile scope** (`mongodb-driver-sync`, `kafka-clients`, `httpclient5`), at the version each catalog is built and tested with, so standalone users get a working catalog. The catalog declares the client, and the application picks the version.
  - An application that uses the client itself, such as Queryeer's MongoDB plugin with its own query engine, declares it directly. Maven's nearest-wins rule bundles the application's version, as one copy.
  - Keep the client versions here current, and change major versions only in step with the known consumers, since a catalog compiled against one major version may not run on another.

### 3.4 The combined artifact

**Decided: `payloadbuilder-catalog` is no longer published.** Consumers switch to the parts. The known consumers are few: Queryeer Desktop, the Swing Queryeer, and the README.

The rejected alternative was to publish it once more as an empty jar that depends on every part, so existing declarations would keep working. It costs one more module and only delays the switch.

### 3.5 Documentation

- **README:** the "Optionally add bundled catalogs" snippet names the per-catalog artifacts. The link "Provided catalogs" points at `payloadbuilder-catalog/`, which still works since the parent directory stays.
- **Release notes for 1.15.0:** `payloadbuilder-catalog` is split. The notes include a table from catalog class to artifact (`ESCatalog` → `payloadbuilder-catalog-es`, and so on) and say that packages are unchanged.

## 4. Steps

1. Create the parent POM and the seven modules. `git mv` the sources, tests and `log4j.properties` (into each module that needs it, or into common's test resources if they share it).
2. Move the dependency versions into the parent's `dependencyManagement`, and declare each module's dependencies from §3.3.
3. Build: `mvn clean verify` from the root.
4. Run `mvn dependency:analyze` per module, and fix used-but-undeclared and declared-but-unused entries.
5. Check isolation with `mvn dependency:tree` per module:
   - `-mongodb` has no kafka-clients, httpclient5 or HikariCP;
   - `-jdbc` has no mongodb-driver or kafka-clients;
   - and so on.
6. Update the README and the release notes.
7. Release 1.15.0.
8. Consumers:
   - Queryeer Desktop's `backend-plugin-payloadbuilder` switches to the parts it bundles today (all six until the engine-catalogs plan moves them out);
   - the Swing Queryeer's `queryeer-catalog` switches to the parts it uses.

## 5. Decisions

Taken 2026-11-19:

| # | Question | Decision |
|---|---|---|
| 1 | Keep publishing `payloadbuilder-catalog` as an aggregator (§3.4) | No. It is no longer published |
| 2 | Module layout | Submodules under `payloadbuilder-catalog/` with a parent POM (§3.1) |
| 3 | Root package classes | One `payloadbuilder-catalog-common` module, with httpclient5 optional (§3.2) |
| 4 | Version | 1.15.0: no API or package changes, only the Maven artifacts. The release notes call out the removed artifact |
