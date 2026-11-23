# Spec: `format = 'auto'` for the Kafka catalog

Status: spec, 2026-11-22, for payloadbuilder 1.17.0. All decisions are taken (§6). Revised the same day: a schema-registry header doesn't mean Avro; the schema's `schemaType` decides (§3, §4).

## 1. Goal

Today a Kafka topic table decodes values as JSON unless the query says otherwise:

```sql
select * from kafka#topic.customers x                         -- fails on the first value that isn't JSON
select * from kafka#topic.customers x with (format = 'raw')    -- works
```

A query over a topic of plain text fails with `Value deserialization error at 0:0 | Failed to deserialize JSON value | JsonParseException: Unrecognized token 'Customer'`. The user has to know each topic's format before querying it. Kafka can't tell them: a topic is bytes, and neither the topic configuration nor the cluster metadata records what format they're in.

After this change a new format, `auto`, looks at each value and decodes it as a schema-registry value (Avro or JSON Schema), JSON or UTF-8 text, whichever it is. `auto` becomes the default, so the first query above works on any topic.

**What doesn't change:**
- `json`, `avro` and `raw` decode exactly as today. An explicit `format = 'json'` still fails on a value that isn't JSON, which is what a user wants when they need to know. The one change to `avro` is a clearer error message for a schema that isn't Avro (§5.3).
- Keys: every format, `auto` included, returns the key as a UTF-8 string, as today.
- `on_error` (`fail`, `skip`, `null`) applies to `auto` the same way as to the other formats.
- The `value` column stays `ResolvedType.ANY`, so a topic that mixes JSON objects and text in one column needs nothing new from the planner.

## 2. Today (1.17.0-SNAPSHOT)

All in `payloadbuilder-catalog/kafka`, package `se.kuseman.payloadbuilder.catalog.kafka`:

| Class | Role |
|---|---|
| `KafkaOptions` | Parses the table's `WITH` options. `FORMAT` defaults to `"json"`. `parseFormat` maps the text to enum `Format { JSON, AVRO, RAW }`; an unknown value throws `Invalid format: '<v>'. Supported: json, avro, raw`. |
| `RecordDeserializerFactory.create(format, session, catalogAlias)` | Switch on `Format`. `AVRO` reads the catalog property `KafkaCatalog.SCHEMA_REGISTRY_URL` and passes it on. |
| `IRecordDeserializer` | `deserializeKey(byte[])`, `deserializeValue(byte[])`, `close()`. Both return `null` for `null` input. |
| `JsonRecordDeserializer` | Jackson `readValue(bytes, Object.class)`; key as UTF-8. Throws `RuntimeException("Failed to deserialize JSON value", e)`. |
| `RawRecordDeserializer` | Key and value as `UTF8String`. |
| `AvroRecordDeserializer(url)` | Throws `IllegalArgumentException` when the URL is missing or blank. Parses the comma-separated registry URLs (trailing `/` stripped). Confluent wire format: byte `0x00`, a 4-byte big-endian schema id, then the payload. Fetches `<registry>/schemas/ids/<id>` (trying each URL in turn, cached per id), parses the response's `schema` field as an Avro schema, and returns the record as a `Map`. It **ignores `schemaType`**: a JSON Schema or Protobuf schema fails in the Avro schema parser and surfaces as `Failed to fetch Avro schema id <id> from all Schema Registry URLs`, although the fetch itself worked. |
| `LazyDeserializingValueVector.getAny(row)` | Decodes a value on first access and caches it. On an exception it counts `nodeData.deserializationErrors`, then throws (`FAIL`) or caches `null` (`SKIP`/`NULL`). |

`avro` is an **optional** dependency of `payloadbuilder-catalog-kafka` (release notes 1.15.0): `AvroRecordDeserializer` can't be loaded when the user hasn't added it.

### The wire format isn't Avro-specific

Confluent's Avro, JSON Schema and Protobuf serializers all write the same header: `0x00`, then the 4-byte schema id. Only the registry knows which one the value is. `GET /schemas/ids/<id>` returns `{"schema": "...", "schemaType": "..."}`, where `schemaType` is absent for Avro (the registry's original format) and `"JSON"` or `"PROTOBUF"` otherwise. What follows the header:

| `schemaType` | Payload after the 5-byte header |
|---|---|
| absent / `AVRO` | Avro binary. Needs the schema to decode. |
| `JSON` | The value as UTF-8 JSON text. Decodes without the schema. |
| `PROTOBUF` | A varint-encoded list of message indexes, then Protobuf binary. Needs the schema and the Protobuf library. |

## 3. Detection, per value

`AutoRecordDeserializer.deserializeValue(bytes)`:

1. `null` → `null`.
2. **Schema-registry wire format:** `bytes.length >= 5 && bytes[0] == 0x00`. Read the schema id from bytes 1–4. Then:
   - **The catalog has a `schema_registry_url`:** look the id up in the registry (§4.1) and route on its `schemaType`:
     - `AVRO` → decode with the Avro deserializer (§4.2).
     - `JSON` → parse the bytes after the header as JSON, with the strict end-of-input check from step 3. A parse failure fails the value with `RuntimeException("Failed to deserialize JSON Schema value (schema id <id>)", e)`.
     - `PROTOBUF` → fail the value with `RuntimeException("Value is a Protobuf schema-registry value (schema id <id>); Protobuf isn't supported")`.
     - Any other `schemaType` → fail the value with `RuntimeException("Value has unsupported schema type '<type>' (schema id <id>)")`.
     - The lookup fails (registry unreachable, unknown id) → the lookup's exception fails the value.

     In every case `on_error` applies to a failed value. A wire-format value never falls back to text when a registry is configured: the registry is authoritative, and showing binary as a garbled string would hide the real problem.
   - **No `schema_registry_url`:** try the bytes after the header as JSON, as in step 3. If they parse to the end of the input → return the parsed value, so JSON Schema values read correctly without a registry. Otherwise → step 4 with the **whole** value, header included. An Avro or Protobuf value read without a registry shows as text, which is no worse than today's error.
3. **JSON:** skip leading whitespace (space, tab, CR, LF) and a UTF-8 BOM (`EF BB BF`). If the first byte left is one of `{ [ "`, `-`, a digit `0-9`, or `t f n` (true/false/null), try Jackson exactly as `JsonRecordDeserializer` does.
   - It parses, and the parser reaches the end of the input (only whitespace after the value) → return the parsed value.
   - It fails, or leaves trailing content → step 4. **No error.** For example, `{not json` and `42 apples` become text.
4. **Text:** return `UTF8String` of the bytes as UTF-8, as `RawRecordDeserializer` does.

Notes:
- Detection is **per value**, with no per-topic or per-partition decision. A topic whose producers changed format over time, or that mixes formats, reads correctly row by row.
- Step 3 pre-checks before parsing, so text costs a few byte comparisons, not a thrown exception, per value. `{ [ "` are recognised by the first byte. `true`, `false` and `null` must be the whole value (whitespace aside). A value starting with `-` or a digit may only contain number characters (`0-9 - + . e E`), so a log line such as `2026-11-22 INFO ...` is rejected at the first space. A value that passes the pre-check but isn't JSON (`1.2.3`, `{not json`) still costs one failed parse.
- 1.16's `json` (Jackson's default) ignores content after the first value: `42 apples` read as `42`. `auto` requires the whole value to be JSON, so such values become text. The release notes call this out.
- Step 2 costs one registry request per distinct schema id per query, not per value (§4.1).
- Text doesn't start with a NUL byte, so step 2 doesn't misfire on text. With no registry, an Avro binary payload that happens to parse completely as JSON would be misread as JSON. That needs the whole binary payload to be valid JSON with nothing after it; in practice it doesn't happen, and configuring the registry removes the guess.
- Text that is itself valid JSON is returned as JSON: `42` as a number, `true` as a boolean, `"x"` as the string `x`. That matches what `format = 'json'` returns for the same bytes. Users who need the exact text use `format = 'raw'`.
- An empty value (`byte[0]`) → step 4: the empty string. Today `json` fails on it.

## 4. Schema registry inside `auto`

### 4.1 `SchemaRegistryClient`

A new package-private class that owns the registry lookup now inside `AvroRecordDeserializer`. It **must not reference any `org.apache.avro` class**, so `auto` can route JSON Schema values when the `avro` library isn't on the classpath.

- Constructor `SchemaRegistryClient(String urls)`: the URL parsing and validation moved from `AvroRecordDeserializer` (comma-separated, trimmed, trailing `/` stripped; `IllegalArgumentException` when missing, blank or empty after parsing).
- `RegisteredSchema get(int schemaId)`: fetches `<registry>/schemas/ids/<id>`, trying each URL in turn as today, and caches the result per id. Returns `record RegisteredSchema(int id, String schemaType, String schema)`, where `schemaType` is `"AVRO"` when the response has no `schemaType`. On failure it throws `RuntimeException("Failed to fetch schema id <id> from all Schema Registry URLs: <urls>", lastException)`. The failure is remembered per id and rethrown for later values without a new HTTP call. A client lives for one datasource execution, so this only lasts for the rest of the query, and it matters with `on_error = 'skip'` or `'null'`: without it every value with the header would make new HTTP calls, each up to the connect (500 ms) or read (15 s) timeout.
- One client per deserializer, as the schema cache is today. `AutoRecordDeserializer` shares its client with its Avro delegate, so an Avro schema id is fetched once.

### 4.2 The Avro delegate

- `AutoRecordDeserializer` creates its `AvroRecordDeserializer` **lazily**, on the first value whose `schemaType` is `AVRO`, so:
  - a topic with no Avro values never needs the Avro library;
  - the `avro` classes are only loaded when a value needs them.
- If the Avro classes are missing (`NoClassDefFoundError` / `LinkageError` while creating or using the delegate), that value fails with `RuntimeException("Value is in Avro schema-registry format (schema id <id>) but the avro library isn't on the classpath")`. Catch the error once, remember it, and fail every later Avro value with the same message without retrying the load. `on_error` then applies as for any failed value. JSON Schema values keep working.
- Every other Avro failure, such as a decode failure, is the delegate's own exception, unchanged. It fails that value and `on_error` applies.
- `close()` closes the Avro delegate if it was created.

## 5. Changes

1. **`KafkaOptions`**
   - `Format` gains `AUTO`.
   - The default changes from `"json"` to `"auto"`.
   - The `parseFormat` error message lists `auto, json, avro, raw`.
2. **New `SchemaRegistryClient`** and `RegisteredSchema` (§4.1).
3. **`AvroRecordDeserializer`**
   - Takes a `SchemaRegistryClient`. Keep the `AvroRecordDeserializer(String url)` constructor, which creates the client, so the explicit `avro` format and the existing tests are unchanged; the `IllegalArgumentException` for a missing URL now comes from the client.
   - Gets the schema through the client, checks `schemaType`, and caches the parsed Avro `Schema` per id. A schema that isn't Avro fails with `RuntimeException("Schema id <id> is <schemaType>, not Avro")`. This replaces today's misleading "Failed to fetch Avro schema" for `format = 'avro'` as well.
   - The URL parsing and the HTTP fetch move to the client.
4. **`RecordDeserializerFactory`:** `case AUTO -> new AutoRecordDeserializer(registryUrl)`, where `registryUrl` is the same lookup `AVRO` does, possibly `null`. `AUTO` must not throw when there's no registry URL; it creates a `SchemaRegistryClient` only when the URL is set. Extract the lookup into a small private method shared by `AVRO` and `AUTO`.
5. **New `AutoRecordDeserializer`** (package-private, next to the others):
   - `deserializeKey`: UTF-8 string, as every other deserializer.
   - `deserializeValue`: §3 and §4.
   - It reuses `JsonRecordDeserializer` and `RawRecordDeserializer` logic rather than copying it. A package-private static helper is fine, for example `JsonRecordDeserializer.tryParse(byte[] bytes, int offset, int length) → Optional<Object>`, used for both the whole value (step 3) and the bytes after the header (step 2).
   - The trailing-content check uses Jackson's parser, so the bytes are parsed once: `createParser(bytes, offset, length)`, read the value, then `nextToken() == null`.
   - A package-private constructor for tests takes the schema lookup (`IntFunction<RegisteredSchema>`, or `null` for no registry) and the Avro delegate supplier (`Supplier<IRecordDeserializer>`). The public-facing constructor `AutoRecordDeserializer(String registryUrl)` builds them from a `SchemaRegistryClient`.
6. **Release notes:** add `documentation/release-notes/1.17.0.md`, or a section in it if it exists, with:
   - the new format, including JSON Schema registry values;
   - **the changed default**, with an example of the one visible difference: a topic of invalid JSON that used to fail now returns text. To get the old behaviour, write `with (format = 'json')`;
   - the clearer `format = 'avro'` error for a schema that isn't Avro.
7. **No other module changes.** Nothing in `payloadbuilder-core` or `payloadbuilder-api`.

## 6. Decisions

| Question | Decision | Why |
|---|---|---|
| Default `auto` or keep `json`? | `auto` | `json` fails on the first topic that isn't JSON, and the user can't know a topic's format from Kafka. An explicit `json` keeps the strict behaviour. |
| Per value or per topic/partition? | Per value | Correct for mixed topics, and the cost is a byte check. A per-topic guess from the first records breaks when formats change over time. |
| Does a wire-format header mean Avro? | No. Route on the registry's `schemaType` | Avro, JSON Schema and Protobuf share the header. Assuming Avro fails every JSON Schema value with a misleading error. |
| JSON Schema values | Supported: the payload after the header, parsed as JSON | It's plain JSON, so it costs nothing extra: no schema, no Avro library. |
| Wire-format value without a registry URL? | Try the payload after the header as JSON; otherwise text of the whole value | JSON Schema values still read correctly. There's nothing to decode Avro or Protobuf with, and text is no worse than today's error. |
| Wire-format value with a registry, decode or lookup fails? | Fail the value (`on_error` applies) | The registry is authoritative and the bytes aren't text; a fallback would hide the cause. |
| Registry lookup lives where? | New `SchemaRegistryClient`, no Avro imports | `auto` must route JSON Schema values when the optional `avro` library is absent. |
| Invalid JSON under `auto`? | Text, no error | `auto` means "whatever it is". The strict check is what `format = 'json'` is for. |
| Keys | Unchanged, UTF-8 | Out of scope; every format does this today. |
| Protobuf registry values | Out of scope, fail with a clear message | Decoding needs the Protobuf library and descriptor handling. The message tells the user why rather than a parse error. |

## 7. Tests

**`AutoRecordDeserializerTest`** (unit, no Kafka, no registry; uses the test constructor from §5.5):
- `null` → `null`.
- JSON object, array, number, `true`, `null` literal, and quoted string → the same results as `JsonRecordDeserializer`.
- Leading whitespace and a UTF-8 BOM before a JSON object → parsed.
- Plain text (`Customer 0`), text starting with a JSON-like character (`{not json`, `42 apples`, `true story`) → `UTF8String` of the input.
- Empty value → empty string.
- No registry:
  - Header + JSON object → the parsed object.
  - Header + bytes that aren't JSON → `UTF8String` of the whole value, no exception.
- With a registry (stub lookup):
  - `schemaType` `AVRO` → handed to the Avro delegate.
  - `schemaType` `JSON` → the payload parsed as JSON; the Avro supplier isn't called.
  - `schemaType` `JSON` with a payload that isn't JSON → throws the "Failed to deserialize JSON Schema value" message.
  - `schemaType` `PROTOBUF` → throws the Protobuf message.
  - The lookup throws → that exception fails the value.
- Avro classes missing (the supplier throws `NoClassDefFoundError`) → that value throws the "avro library isn't on the classpath" message, a second Avro value throws the same message without calling the supplier again, and a `JSON`-typed value still parses.
- `close()` closes a created delegate and doesn't create one.

**`SchemaRegistryClientTest`:**
- The URL validation and normalization tests moved from `AvroRecordDeserializerTest` (null, blank, `"  ,  , "`, comma-separated, spaces, trailing `/`).
- Response parsing: no `schemaType` → `AVRO`; `"JSON"` and `"PROTOBUF"` kept as given. Extract the response-to-`RegisteredSchema` mapping into a static method so it's testable without HTTP.

**`AvroRecordDeserializerTest`:** existing tests keep passing. Add: a schema whose `schemaType` is `JSON` → throws `Schema id <id> is JSON, not Avro` (stub client).

**`KafkaOptionsTest`:**
- No `format` option → `AUTO`.
- `format = 'auto'` (any case) → `AUTO`.
- The error message for an unknown format lists `auto`.

**`KafkaIntegrationTest`** (Testcontainers, as the existing tests):
- A topic with mixed JSON and text values, read without a `format` option: each row's `value` is the parsed JSON or the text.
- The same topic with `format = 'json'` still fails with today's error (default `on_error`).

**`KafkaAvroIntegrationTest`:**
- Reading the existing Avro topic **without** a `format` option returns the same rows as `format = 'avro'`.
- A topic written with JSON Schema wire format (register a JSON schema with the registry, produce header + JSON bytes directly, so no JSON Schema serializer dependency is needed), read without a `format` option → the JSON values. With `format = 'avro'` → fails with the "not Avro" message.

Run the module's tests and `checkstyle` as the build does: `mvnw -pl payloadbuilder-catalog/kafka -am verify`.

## 8. Out of scope

- Detecting the key's format.
- Decoding Protobuf registry values (§6).
- Changing explicit `format = 'json'` to accept JSON Schema wire-format values. `auto` covers them.
- Telling the user which format each row was decoded as, for example as a column. A later change can add one if it's needed.
- Anything in Queryeer. Queryeer's Kafka plugin passes no `format`, so it gets `auto` on upgrade.
