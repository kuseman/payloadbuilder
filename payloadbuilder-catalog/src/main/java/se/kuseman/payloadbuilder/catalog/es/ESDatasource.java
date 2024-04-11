package se.kuseman.payloadbuilder.catalog.es;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.catalog.es.ESQueryUtils.getMgetUrl;
import static se.kuseman.payloadbuilder.catalog.es.ESQueryUtils.getScrollUrl;
import static se.kuseman.payloadbuilder.catalog.es.ESQueryUtils.getSearchUrl;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.AbstractHttpEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.CatalogException;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.IPredicate.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.ISeekKey;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.SeekType;
import se.kuseman.payloadbuilder.api.execution.NodeData;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.catalog.es.ESQueryUtils.SortItemMeta;
import se.kuseman.payloadbuilder.catalog.es.ElasticsearchMetaUtils.MappedProperty;

/** Operator for Elastic search */
class ESDatasource implements IDatasource
{
    static final String ID = "_id";

    static final QualifiedName INDEX = QualifiedName.of("__index");
    static final QualifiedName TYPE = QualifiedName.of("__type");
    static final String DOCID_COLUMN = "__id";
    static final QualifiedName DOCID = QualifiedName.of(DOCID_COLUMN);
    static final QualifiedName META = QualifiedName.of("__meta");
    static final ObjectMapper MAPPER;
    private static final ObjectReader READER;

    static
    {
        MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        MAPPER.getFactory()
                // NOTE! Found some really weird ELK log post where a dynamic mapping had created
                // a 65000-ish char long field name and PLB wasn't able to parse that data.
                .setStreamReadConstraints(StreamReadConstraints.builder()
                        .maxNameLength(100_000)
                        .build());
        READER = MAPPER.readerFor(ESResponse.class);
    }

    private final int nodeId;
    private final ElasticStrategy strategy;
    private final String catalogAlias;
    private final QualifiedName table;
    private final ISeekPredicate indexPredicate;
    /** The mapped property used for the index */
    private final MappedProperty indexProperty;
    private final List<PropertyPredicate> propertyPredicates;
    private final List<SortItemMeta> sortItems;

    ESDatasource(int nodeId, ElasticStrategy strategy, String catalogAlias, QualifiedName table, ISeekPredicate indexPredicate, MappedProperty indexProperty,
            List<PropertyPredicate> propertyPredicates, List<SortItemMeta> sortItems)
    {
        this.nodeId = nodeId;
        this.strategy = strategy;
        this.catalogAlias = catalogAlias;
        this.table = table;
        this.indexPredicate = indexPredicate;
        this.indexProperty = indexProperty;
        this.propertyPredicates = requireNonNull(propertyPredicates, "propertyPredicates");
        this.sortItems = requireNonNull(sortItems, "sortItems");
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        String indexField = getIndexField();
        boolean quoteValues = indexProperty == null
                || indexProperty.shouldQuoteValues();

        Map<String, Object> result = ofEntries(true, entry(CATALOG, ESCatalog.NAME), entry(PREDICATE, propertyPredicates.stream()
                .map(PropertyPredicate::getDescription)
                .collect(joining(" AND "))), entry("Sort",
                        sortItems.stream()
                                .map(Object::toString)
                                .collect(joining(", "))),
                entry("Query", ESQueryUtils.getSearchBody(true, strategy, sortItems, propertyPredicates, null, indexField, quoteValues, context)));

        Data data = context.getStatementContext()
                .getNodeData(nodeId);
        if (data != null)
        {
            result.put("Request count", data.requestCount);
            result.put("Scroll count", data.scrollCount);
            result.put("Bytes sent", FileUtils.byteCountToDisplaySize(data.bytesSent));
            result.put("Bytes received", FileUtils.byteCountToDisplaySize(data.bytesReceived));
            result.put("Request and deserialization time", DurationFormatUtils.formatDurationHMS(data.requestTime.getTime(TimeUnit.MILLISECONDS)));
        }

        return result;
    }

    @Override
    public TupleIterator execute(IExecutionContext context, IDatasourceOptions options)
    {
        ValueVector indexSeekValues = getIndexSeekValues(context);
        String indexField = getIndexField();
        int batchSize = getBatchSize(context, options, indexField, indexSeekValues);

        ESType esType = ESType.of(context.getSession(), catalogAlias, table);
        Data data = context.getStatementContext()
                .getOrCreateNodeData(nodeId, Data::new);

        // mget index
        if (useMgets(esType, indexField))
        {
            return getMgetIterator(context, esType, data, batchSize);
        }

        boolean doScroll = batchSize > 0;
        String searchUrl = getSearchUrl(esType.endpoint, esType.index, strategy.supportsTypes() ? esType.type
                : null,
                doScroll ? batchSize
                        : null,
                doScroll ? 2
                        : null,
                false);
        String scrollUrl = doScroll ? getScrollUrl(esType.endpoint, 2)
                : null;

        boolean quoteValues = indexProperty == null
                || indexProperty.shouldQuoteValues();
        String body = ESQueryUtils.getSearchBody(false, strategy, sortItems, propertyPredicates, indexSeekValues, indexField, quoteValues, context);

        return getScrollingIterator(context, strategy, catalogAlias, esType.endpoint, data, searchUrl, scrollUrl, body);
    }

    private int getBatchSize(IExecutionContext context, IDatasourceOptions options, String indexField, ValueVector indexSeekKeys)
    {
        // Fetch batch size from options
        int batchSize = options.getBatchSize(context);

        // If this is an index request, see if the index size is lower or equal to the batch
        // size option then we can skip scrolling
        int min = -1;
        if (ID.equalsIgnoreCase(indexField))
        {
            min = indexSeekKeys.size();
        }

        // Filter on id, see if we can skip scroll
        for (PropertyPredicate predicate : propertyPredicates)
        {
            if (ID.equalsIgnoreCase(predicate.property))
            {
                if (predicate.predicate.getType() == Type.COMPARISION
                        && predicate.predicate.getComparisonType() == IComparisonExpression.Type.EQUAL)
                {
                    min = min < 0 ? 1
                            : Math.min(min, 1);
                }
                else if (predicate.predicate.getType() == Type.IN
                        && !predicate.predicate.getInExpression()
                                .isNot())
                {
                    int argSize = predicate.predicate.getInExpression()
                            .getArguments()
                            .size();
                    min = min < 0 ? argSize
                            : Math.min(min, argSize);
                }
            }
        }

        // We have a filter on Id that is less than batch size => don't use scroll
        if (min >= 0
                && min <= batchSize)
        {
            return -1;
        }

        return batchSize;
    }

    private ValueVector getIndexSeekValues(IExecutionContext context)
    {
        if (indexPredicate == null)
        {
            return null;
        }

        List<ISeekKey> seekKeys = indexPredicate.getSeekKeys(context);
        if (seekKeys.size() != 1
                || seekKeys.get(0)
                        .getType() != SeekType.EQ)
        {
            throw new IllegalArgumentException("Elastic catalog only supports single EQUAL seek keys");
        }

        return seekKeys.get(0)
                .getValue();
    }

    private String getIndexField()
    {
        if (indexProperty == null)
        {
            return null;
        }

        String column = indexProperty.name.toDotDelimited();

        // Fix field name for special fields
        if (DOCID_COLUMN.equalsIgnoreCase(column))
        {
            return ID;
        }
        else if (indexProperty.isFreeTextMapping())
        {
            // Translate to non freetext
            MappedProperty nonFreeTextField = indexProperty.getNonFreeTextField();
            return nonFreeTextField != null ? nonFreeTextField.name.toDotDelimited()
                    : null;
        }

        return column;
    }

    private boolean useMgets(ESType esType, String indexField)
    {
        // _id index field and no multi index query => mgets
        return ID.equalsIgnoreCase(indexField)
                && !esType.index.contains("*")
                && !esType.index.contains(",");
    }

    private TupleIterator getMgetIterator(IExecutionContext context, ESType esType, Data data, int batchSize)
    {
        MutableBoolean doRequest = new MutableBoolean(true);
        final String mgetUrl = getMgetUrl(esType.endpoint, esType.index, strategy.supportsTypes() ? esType.type
                : null);
        final MutableInt batch = new MutableInt(0);
        final ValueVector values = indexPredicate.getSeekKeys(context)
                .get(0)
                .getValue();

        return getIterator(context, strategy, catalogAlias, esType.endpoint, data, scrollId ->
        {
            if (!doRequest.booleanValue())
            {
                return null;
            }

            int from = batchSize < 0 ? 0
                    : batch.getAndIncrement() * batchSize;
            int to = batchSize < 0 ? values.size()
                    : Math.min(from + batchSize, values.size());

            DocIdStreamingEntity entity = new DocIdStreamingEntity(values, data, from, to);
            data.bytesSent += mgetUrl.length();
            HttpPost post = new HttpPost(mgetUrl);
            post.setEntity(entity);

            // Done, no more requests
            if (to == values.size())
            {
                doRequest.setFalse();
            }
            return post;
        });
    }

    //@formatter:off
    static TupleIterator getScrollingIterator(
            IExecutionContext context,
            ElasticStrategy strategy,
            String catalogAlias,
            String endpoint,
            ESDatasource.Data data,
            String searchUrl,
            String scrollUrl,
            String body)
    //@formatter:on
    {
        final MutableBoolean doRequest = new MutableBoolean(true);
        return getIterator(context, strategy, catalogAlias, endpoint, data, scrollId ->
        {
            if (doRequest.booleanValue())
            {
                data.bytesSent += searchUrl.length() + body.length();
                HttpPost post = new HttpPost(searchUrl);
                post.setEntity(new StringEntity(body, UTF_8));
                doRequest.setFalse();
                return post;
            }
            else if (scrollId.getValue() != null)
            {
                data.scrollCount++;
                String id = scrollId.getValue();
                data.bytesSent += scrollUrl.length() + body.length();
                scrollId.setValue(null);
                return strategy.getScrollRequest(scrollUrl, id);
            }

            return null;
        });
    }

    static TupleIterator getIterator(IExecutionContext context, ElasticStrategy strategy, String catalogAlias, String endpoint, Data data,
            Function<MutableObject<String>, HttpUriRequestBase> requestSupplier)
    {
        // CSOFF
        return new TupleIterator()
        // CSON
        {
            private Set<String> addedColumns;
            private String[] vectorColumns;
            private final MutableObject<String> scrollId = new MutableObject<>();
            private TupleVector next;
            private boolean closed;
            private volatile HttpUriRequestBase currentRequest;
            private volatile boolean abort;
            private final Runnable abortListener = new Runnable()
            {
                @Override
                public void run()
                {
                    HttpUriRequestBase r = currentRequest;
                    abort = true;
                    if (r != null)
                    {
                        try
                        {
                            r.abort();
                        }
                        catch (UnsupportedOperationException e)
                        {
                        }
                    }
                }
            };
            {
                context.getSession()
                        .registerAbortListener(abortListener);
            }

            @Override
            public boolean hasNext()
            {
                return !closed
                        && setNext();
            }

            @Override
            public TupleVector next()
            {
                if (next == null)
                {
                    throw new NoSuchElementException();
                }
                TupleVector result = next;
                next = null;
                return result;
            }

            @Override
            public void close()
            {
                context.getSession()
                        .unregisterAbortListener(abortListener);

                if (!isBlank(scrollId.getValue()))
                {
                    data.requestCount++;

                    ClassicHttpRequest delete = strategy.getDeleteScrollRequest(endpoint, scrollId.getValue());
                    try
                    {
                        HttpClientUtils.execute(context.getSession(), catalogAlias, delete, response ->
                        {
                            HttpEntity entity = response.getEntity();
                            int status = response.getCode();
                            if (!(status == HttpStatus.SC_OK
                                    || status == HttpStatus.SC_NOT_FOUND))
                            {
                                String body = IOUtils.toString(entity.getContent(), StandardCharsets.UTF_8);
                                throw new RuntimeException("Error clearing scroll: " + body);
                            }
                            return null;
                        });
                    }
                    catch (Exception e)
                    {
                        if (e instanceof RuntimeException)
                        {
                            throw (RuntimeException) e;
                        }
                        throw new RuntimeException("Error deleting scroll", e);
                    }
                }
                closed = true;
            }

            // CSOFF
            private boolean setNext()
            // CSON
            {
                while (next == null)
                {
                    if (abort)
                    {
                        return false;
                    }

                    currentRequest = requestSupplier.apply(scrollId);
                    if (currentRequest == null)
                    {
                        return false;
                    }

                    ESResponse esReponse;
                    data.requestCount++;
                    data.requestTime.resume();

                    try
                    {
                        esReponse = HttpClientUtils.execute(context.getSession(), catalogAlias, currentRequest, response ->
                        {
                            HttpEntity entity = response.getEntity();
                            if (response.getCode() != HttpStatus.SC_OK)
                            {
                                String body = IOUtils.toString(response.getEntity()
                                        .getContent(), StandardCharsets.UTF_8);
                                try
                                {
                                    // Try to parse and format json error
                                    Object parsedBody = MAPPER.readValue(body, Object.class);
                                    body = MAPPER.writerWithDefaultPrettyPrinter()
                                            .writeValueAsString(parsedBody);
                                }
                                catch (IOException e)
                                {
                                    // SWALLOW
                                }

                                throw new RuntimeException("Error query Elastic. Status: " + response.getCode() + "." + System.lineSeparator() + body);
                            }
                            CountingInputStream cis = new CountingInputStream(entity.getContent());
                            ESResponse result = READER.readValue(cis);
                            data.bytesReceived += cis.getByteCount();
                            return result;
                        });
                    }
                    catch (CatalogException e)
                    {
                        throw e;
                    }
                    catch (Exception e)
                    {
                        // Only throw if we didn't abort the query
                        if (!abort)
                        {
                            throw new RuntimeException("Error query", e);
                        }
                        return false;
                    }
                    finally
                    {
                        data.requestTime.suspend();
                    }

                    scrollId.setValue(esReponse.scrollId);

                    List<Doc> docs = esReponse.getDocs();
                    docs.removeIf(d -> !d.isValid());

                    if (docs.isEmpty())
                    {
                        return false;
                    }

                    boolean columnsChanged = false;
                    if (addedColumns == null)
                    {
                        Set<String> keySet = docs.get(0).source.keySet();
                        addedColumns = new LinkedHashSet<>(keySet.size() + 4);
                        addedColumns.add(INDEX.toString());
                        addedColumns.add(TYPE.toString());
                        addedColumns.add(DOCID.toString());
                        addedColumns.add(META.toString());
                        columnsChanged = true;
                    }

                    for (Doc doc : docs)
                    {
                        if (addedColumns.addAll(doc.source.keySet()))
                        {
                            columnsChanged = true;
                        }
                    }

                    if (columnsChanged)
                    {
                        vectorColumns = addedColumns.toArray(EMPTY_STRING_ARRAY);
                    }

                    next = new DocTupleVector(docs, vectorColumns);
                }
                return true;
            }
        };
    }

    private static class DocTupleVector implements TupleVector
    {
        private final List<Doc> docs;
        private final String[] columns;
        private final Schema schema;

        DocTupleVector(List<Doc> docs, String[] columns)
        {
            this.docs = docs;
            this.columns = columns;
            this.schema = new Schema(Arrays.stream(columns)
                    .map(c -> Column.of(c, ResolvedType.of(Column.Type.Any)))
                    .collect(toList()));
        }

        @Override
        public Schema getSchema()
        {
            return schema;
        }

        @Override
        public int getRowCount()
        {
            return docs.size();
        }

        @Override
        public ValueVector getColumn(int column)
        {
            return new ValueVector()
            {
                @Override
                public ResolvedType type()
                {
                    return ResolvedType.of(Column.Type.Any);
                }

                @Override
                public int size()
                {
                    return docs.size();
                }

                @Override
                public boolean isNull(int row)
                {
                    return getAny(row) == null;
                }

                @Override
                public Object getAny(int row)
                {
                    Doc doc = docs.get(row);

                    // INDEX
                    if (column == 0)
                    {
                        return doc.meta.get("_index");
                    }
                    // TYPE
                    else if (column == 1)
                    {
                        return doc.meta.get("_type");
                    }
                    // DOCID
                    else if (column == 2)
                    {
                        return doc.meta.get("_id");
                    }
                    // META
                    else if (column == 3)
                    {
                        return doc.meta;
                    }

                    String colName = columns[column];
                    return doc.source.get(colName);
                }
            };
        }
    }

    /** Top response (Used both in get request and search request) */
    private static class ESResponse
    {
        @JsonProperty("_scroll_id")
        String scrollId;

        /** Get response */
        @JsonProperty("docs")
        List<Doc> docs;

        /** Search response */
        @JsonProperty("hits")
        OuterHits hits;

        /** Return docs fr */
        List<Doc> getDocs()
        {
            if (docs != null)
            {
                return docs;
            }
            else if (hits != null)
            {
                return hits.hits;
            }

            return emptyList();
        }
    }

    /** Outer hits */
    private static class OuterHits
    {
        @JsonProperty("hits")
        List<Doc> hits;
    }

    /** Doc. Put _source on root and gather all other values on the same level in meta map */
    @JsonDeserialize(
            using = DocDeserializer.class)
    private static class Doc
    {
        Map<String, Object> source;
        /** All fields on hit that isn't _source are put in meta */
        Map<String, Object> meta;

        void putMeta(String name, Object value)
        {
            if (meta == null)
            {
                meta = new HashMap<>();
            }
            meta.put(name, value);
        }

        boolean isValid()
        {
            // mget => found
            // search =>
            if (source == null
                    || meta == null)
            {
                return false;
            }

            Boolean found = (Boolean) meta.get("found");
            return found == null
                    || found;
        }
    }

    /** Deserializer for doc element */
    private static class DocDeserializer extends JsonDeserializer<Doc>
    {
        @SuppressWarnings("unchecked")
        @Override
        public Doc deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException
        {
            Doc doc = new Doc();

            p.nextToken(); // START_OBJECT
            while (p.currentToken() != JsonToken.END_OBJECT)
            {
                String name = p.currentName();
                if ("_source".equals(name))
                {
                    p.nextToken();
                    doc.source = p.readValueAs(Map.class);
                }
                else if (name != null)
                {
                    p.nextToken();
                    doc.putMeta(name, p.readValueAs(Object.class));
                }
                p.nextToken();
            }
            return doc;
        }
    }

    @Override
    public String toString()
    {
        return String.format("ID: %d, %s", nodeId, (indexPredicate != null ? "index"
                : "scan") + " (" + table + ")");
    }

    /** Node data with stats */
    static class Data extends NodeData
    {
        StopWatch requestTime = new StopWatch();
        int requestCount;
        long bytesSent;
        long bytesReceived;
        int scrollCount;

        Data()
        {
            requestTime.start();
            requestTime.suspend();
        }
    }

    /** Class the streams doc id's to _mget endpoint in ES */
    private static class DocIdStreamingEntity extends AbstractHttpEntity
    {
        private static final byte[] HEADER_BYTES = "{\"ids\":[".getBytes(StandardCharsets.UTF_8);
        private static final byte[] FOOTER_BYTES = "]}".getBytes(StandardCharsets.UTF_8);
        private static final byte[] QUOTE_BYTES = "\"".getBytes(StandardCharsets.UTF_8);
        private static final byte[] COMMA_BYTES = ",".getBytes(StandardCharsets.UTF_8);

        private final ValueVector values;
        private final Data data;
        private final int from;
        private final int to;

        private DocIdStreamingEntity(ValueVector values, Data data, int from, int to)
        {
            super(ContentType.APPLICATION_JSON, null);
            this.values = values;
            this.data = data;
            this.from = from;
            this.to = to;
        }

        @Override
        public boolean isRepeatable()
        {
            return false;
        }

        @Override
        public long getContentLength()
        {
            return -1;
        }

        @Override
        public InputStream getContent() throws IOException, UnsupportedOperationException
        {
            return null;
        }

        @Override
        public void writeTo(OutputStream outStream) throws IOException
        {
            try (CountingOutputStream bos = new CountingOutputStream(outStream))
            {
                bos.write(HEADER_BYTES);

                boolean first = true;
                for (int i = from; i < to; i++)
                {
                    if (values.isNull(i))
                    {
                        continue;
                    }

                    if (!first)
                    {
                        bos.write(COMMA_BYTES);
                    }
                    first = false;
                    bos.write(QUOTE_BYTES);
                    String docId = String.valueOf(values.valueAsObject(i));
                    bos.write(docId.getBytes(StandardCharsets.UTF_8));
                    bos.write(QUOTE_BYTES);
                }

                bos.write(FOOTER_BYTES);
                data.bytesSent += bos.getByteCount();
            }
        }

        @Override
        public boolean isStreaming()
        {
            return false;
        }

        @Override
        public void close() throws IOException
        {
        }
    }
}
