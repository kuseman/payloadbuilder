package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.catalog.es.ESCatalog.SINGLE_TYPE_TABLE_NAME;
import static se.kuseman.payloadbuilder.catalog.es.HttpClientUtils.execute;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.AbstractHttpEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.message.BasicHeader;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.CatalogException;
import se.kuseman.payloadbuilder.api.operator.AOperator;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.IIndexPredicate;
import se.kuseman.payloadbuilder.api.operator.IOrdinalValues;
import se.kuseman.payloadbuilder.api.operator.NodeData;
import se.kuseman.payloadbuilder.api.operator.Row;
import se.kuseman.payloadbuilder.api.utils.StringUtils;
import se.kuseman.payloadbuilder.catalog.es.ESCatalog.MappedProperty;
import se.kuseman.payloadbuilder.catalog.es.ESUtils.SortItemMeta;

/** Operator for Elastic search */
class ESOperator extends AOperator
{
    static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final String ID = "_id";

    static final String INDEX = "__index";
    static final String TYPE = "__type";
    static final String DOCID = "__id";
    static final String META = "__meta";
    static final ObjectMapper MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final ObjectReader READER = MAPPER.readerFor(ESResponse.class);

    private final TableAlias tableAlias;
    private final IIndexPredicate indexPredicate;
    /** The mapped property used for the index */
    private final MappedProperty indexProperty;
    private final String catalogAlias;
    private final List<PropertyPredicate> propertyPredicates;
    private final List<SortItemMeta> sortItems;

    ESOperator(int nodeId, String catalogAlias, TableAlias tableAlias, IIndexPredicate indexPredicate, MappedProperty indexProperty, List<PropertyPredicate> propertyPredicates,
            List<SortItemMeta> sortItems)
    {
        super(nodeId);
        this.catalogAlias = catalogAlias;
        this.tableAlias = tableAlias;
        this.indexPredicate = indexPredicate;
        this.indexProperty = indexProperty;
        this.propertyPredicates = requireNonNull(propertyPredicates, "propertyPredicates");
        this.sortItems = requireNonNull(sortItems, "sortItems");
    }

    @Override
    public String getName()
    {
        return (indexPredicate != null ? "index"
                : "scan")
               + " ("
               + tableAlias.getTable()
               + ")";
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        ESType esType = ESType.of(context.getSession(), catalogAlias, tableAlias.getTable());

        String indexField = getIndexField();
        boolean quoteValues = indexProperty == null
                || indexProperty.shouldQuoteValues();

        Map<String, Object> result = ofEntries(true, entry(CATALOG, ESCatalog.NAME), entry(PREDICATE, propertyPredicates.stream()
                .map(PropertyPredicate::getDescription)
                .collect(joining(" AND "))), entry("Sort",
                        sortItems.stream()
                                .map(Object::toString)
                                .collect(joining(", "))),
                entry("Query", ESUtils.getSearchBody(sortItems, propertyPredicates, indexPredicate, indexField, quoteValues, SINGLE_TYPE_TABLE_NAME.equals(esType.type), context)));

        if (indexPredicate != null)
        {
            result.put(INDEX, indexPredicate.getIndex());
        }

        Data data = context.getStatementContext()
                .getNodeData(nodeId);
        if (data != null)
        {
            result.put("Request count", data.requestCount);
            result.put("Bytes sent", FileUtils.byteCountToDisplaySize(data.bytesSent));
            result.put("Bytes received", FileUtils.byteCountToDisplaySize(data.bytesReceived));
            result.put("Request and deserialization time", DurationFormatUtils.formatDurationHMS(data.requestTime.getTime(TimeUnit.MILLISECONDS)));
        }

        return result;
    }

    @Override
    public TupleIterator open(IExecutionContext context)
    {
        ESType esType = ESType.of(context.getSession(), catalogAlias, tableAlias.getTable());
        String indexField = getIndexField();
        Data data = context.getStatementContext()
                .getOrCreateNodeData(nodeId, Data::new);
        // mget index
        if (useMgets(esType, indexField))
        {
            return getMgetIndexOperator(context, esType, data);
        }

        boolean isSingleType = ESCatalog.SINGLE_TYPE_TABLE_NAME.equals(esType.type);
        final String searchUrl = getSearchUrl(esType.endpoint, esType.index, esType.type, 1000, 2, indexField);
        String scrollUrl = getScrollUrl(esType.endpoint, esType.type, 2, indexField);

        boolean quoteValues = indexProperty == null
                || indexProperty.shouldQuoteValues();
        String body = ESUtils.getSearchBody(sortItems, propertyPredicates, indexPredicate, indexField, quoteValues, isSingleType, context);
        return getIterator(context, catalogAlias, tableAlias, esType.endpoint, isSingleType, data, scrollId ->
        {
            if (scrollId.getValue() == null)
            {
                data.bytesSent += searchUrl.length() + body.length();
                HttpPost post = new HttpPost(searchUrl);
                post.setEntity(new StringEntity(body, UTF_8));
                return post;
            }
            else
            {
                String id = scrollId.getValue();
                data.bytesSent += scrollUrl.length() + body.length();
                scrollId.setValue(null);
                HttpPost post = new HttpPost(scrollUrl);
                if (isSingleType)
                {
                    post.setEntity(new StringEntity("{\"scroll_id\":\"" + id + "\" }", UTF_8));
                }
                else
                {
                    post.removeHeaders(HttpHeaders.CONTENT_TYPE);
                    post.setEntity(new StringEntity(id, UTF_8));
                }
                return post;
            }
        });
    }

    private String getIndexField()
    {
        if (indexPredicate == null)
        {
            return null;
        }

        String column = indexPredicate.getIndexColumns()
                .get(0);
        String field = column;

        // Fix field name for special fields
        if (DOCID.equalsIgnoreCase(column))
        {
            field = ID;
        }
        else if (indexProperty.isFreeTextMapping())
        {
            // Translate to non freetext
            MappedProperty nonFreeTextField = indexProperty.getNonFreeTextField();
            field = nonFreeTextField.name;
        }

        return field;
    }

    private boolean useMgets(ESType esType, String indexField)
    {
        // _id index field and no multi index query => mgets
        return ID.equalsIgnoreCase(indexField)
                && !esType.index.contains("*")
                && !esType.index.contains(",");
    }

    private TupleIterator getMgetIndexOperator(IExecutionContext context, ESType esType, Data data)
    {
        MutableBoolean doRequest = new MutableBoolean(true);
        String mgetUrl = String.format("%s/%s/%s/_mget?filter_path=_scroll_id,hits.hits", esType.endpoint, esType.index, esType.type);
        DocIdStreamingEntity entity = new DocIdStreamingEntity(indexPredicate, context, data);
        return getIterator(context, catalogAlias, tableAlias, esType.endpoint, ESCatalog.SINGLE_TYPE_TABLE_NAME.equals(esType.type), data, scrollId ->
        {
            if (doRequest.isTrue())
            {
                data.bytesSent += mgetUrl.length();
                HttpPost post = new HttpPost(mgetUrl);
                post.setEntity(entity);
                doRequest.setFalse();
                return post;
            }

            return null;
        });
    }

    static String getSearchUrl(String endpoint, String index, String type, Integer size, Integer scrollMinutes, String indexField)
    {
        StringUtils.requireNonBlank(endpoint, "endpoint is required");
        return String.format("%s/%s%s/_search?%s%s&filter_path=_scroll_id,hits.hits", endpoint, isBlank(index) ? "*"
                : index,
                isBlank(type) ? ""
                        : ("/" + type),
                scrollMinutes != null ? ("scroll=" + scrollMinutes + "m")
                        : "",
                size != null ? ("&size=" + size)
                        : "");
    }

    static String getScrollUrl(String endpoint, String type, int scrollMinutes, String indexField)
    {
        StringUtils.requireNonBlank(endpoint, "endpoint is required");
        return String.format("%s/_search/scroll?scroll=%dm&filter_path=_scroll_id,hits.hits", endpoint, scrollMinutes);
    }

    static TupleIterator getIterator(IExecutionContext context, String catalogAlias, TableAlias tableAlias, String endpoint, boolean isSingleType, Data data,
            Function<MutableObject<String>, HttpRequestBase> requestSupplier)
    {
        // CSOFF
        return new TupleIterator()
        // CSON
        {
            private Set<String> addedColumns;
            private String[] rowColumns;
            private final MutableObject<String> scrollId = new MutableObject<>();
            private Iterator<Doc> docIt;
            private Row next;
            private boolean closed;

            @Override
            public boolean hasNext()
            {
                return !closed
                        && setNext();
            }

            @Override
            public Row next()
            {
                Row result = next;
                next = null;
                return result;
            }

            @Override
            public void close()
            {
                if (!isBlank(scrollId.getValue()))
                {
                    closed = true;
                    data.requestCount++;
                    HttpDeleteWithBody delete = new HttpDeleteWithBody(endpoint + "/_search/scroll");

                    // TODO: this doesn't work that good now
                    // need to have ES version to correctly perform requests
                    // Version 6.6
                    if (isSingleType)
                    {
                        delete.setEntity(new StringEntity("{\"scroll_id\":\"" + scrollId.getValue() + "\"}", StandardCharsets.UTF_8));
                    }
                    else
                    {
                        delete.setEntity(new StringEntity(scrollId.getValue(), StandardCharsets.UTF_8));
                    }

                    HttpEntity entity = null;
                    try (CloseableHttpResponse response = execute(context.getSession(), catalogAlias, (delete)))
                    {
                        entity = response.getEntity();
                        int status = response.getStatusLine()
                                .getStatusCode();
                        if (!(status == HttpStatus.SC_OK
                                || status == HttpStatus.SC_NOT_FOUND))
                        {
                            String body = IOUtils.toString(entity.getContent(), StandardCharsets.UTF_8);
                            throw new RuntimeException("Error clearing scroll: " + body);
                        }
                    }
                    catch (Exception e)
                    {
                        if (e instanceof RuntimeException)
                        {
                            throw (RuntimeException) e;
                        }
                        throw new RuntimeException("Error deleting scroll", e);
                    }
                    finally
                    {
                        EntityUtils.consumeQuietly(entity);
                    }
                }
            }

            // CSOFF
            private boolean setNext()
            // CSON
            {
                while (next == null)
                {
                    if (context.getSession()
                            .abortQuery())
                    {
                        return false;
                    }

                    if (docIt == null)
                    {
                        HttpRequestBase request = requestSupplier.apply(scrollId);
                        if (request == null)
                        {
                            return false;
                        }

                        ESResponse esReponse;
                        data.requestCount++;
                        data.requestTime.resume();
                        HttpEntity entity = null;
                        try (CloseableHttpResponse response = execute(context.getSession(), catalogAlias, request))
                        {
                            entity = response.getEntity();
                            if (response.getStatusLine()
                                    .getStatusCode() != HttpStatus.SC_OK)
                            {
                                throw new RuntimeException("Error query Elastic. Status: " + response.getStatusLine()
                                                           + ". "
                                                           + IOUtils.toString(response.getEntity()
                                                                   .getContent(), StandardCharsets.UTF_8));
                            }
                            CountingInputStream cis = new CountingInputStream(entity.getContent());
                            esReponse = READER.readValue(cis);
                            data.bytesReceived += cis.getByteCount();
                        }
                        catch (CatalogException e)
                        {
                            throw e;
                        }
                        catch (IOException e)
                        {
                            throw new RuntimeException("Error query", e);
                        }
                        finally
                        {
                            EntityUtils.consumeQuietly(entity);
                            data.requestTime.suspend();
                        }

                        scrollId.setValue(esReponse.scrollId);
                        docIt = esReponse.getDocs()
                                .iterator();

                        if (!docIt.hasNext())
                        {
                            return false;
                        }
                        continue;
                    }
                    else if (!docIt.hasNext())
                    {
                        docIt = null;
                        continue;
                    }

                    Doc doc = docIt.next();
                    if (!doc.isValid())
                    {
                        continue;
                    }

                    if (addedColumns == null)
                    {
                        Set<String> keySet = doc.source.keySet();
                        // CSOFF
                        addedColumns = new LinkedHashSet<>(keySet.size() + 4);
                        // CSON
                        addedColumns.add(INDEX);
                        addedColumns.add(TYPE);
                        addedColumns.add(DOCID);
                        addedColumns.add(META);
                        addedColumns.addAll(keySet);
                        rowColumns = addedColumns.toArray(EMPTY_STRING_ARRAY);
                    }
                    else if (addedColumns.addAll(doc.source.keySet()))
                    {
                        rowColumns = addedColumns.toArray(EMPTY_STRING_ARRAY);
                    }
                    next = Row.of(tableAlias, rowColumns, new DocValues(doc, rowColumns));
                }
                return true;
            }
        };
    }

    /** DocValues. Wrapping a {@link Doc} to support index,type,id besides source fields */
    private static class DocValues implements Row.RowValues
    {
        private final Doc doc;
        private final String[] columns;

        DocValues(Doc doc, String[] columns)
        {
            this.doc = doc;
            this.columns = columns;
        }

        @Override
        public Object getValue(int ordinal)
        {
            if (ordinal == -1)
            {
                return null;
            }
            else if (ordinal == 0)
            {
                return doc.meta.get("_index");
            }
            else if (ordinal == 1)
            {
                return doc.meta.get("_type");
            }
            else if (ordinal == 2)
            {
                return doc.meta.get("_id");
            }
            else if (ordinal == 3)
            {
                return doc.meta;
            }
            return doc.source.get(columns[ordinal]);
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
                : "scan") + " (" + tableAlias.toString() + ")");
    }

    /** Node data with stats */
    static class Data extends NodeData
    {
        StopWatch requestTime = new StopWatch();
        int requestCount;
        long bytesSent;
        long bytesReceived;

        Data()
        {
            requestTime.start();
            requestTime.suspend();
        }
    }

    /** Entity support for DELETE */
    private static class HttpDeleteWithBody extends HttpEntityEnclosingRequestBase
    {
        public static final String METHOD_NAME = "DELETE";

        @Override
        public String getMethod()
        {
            return METHOD_NAME;
        }

        HttpDeleteWithBody(final String uri)
        {
            super();
            setURI(URI.create(uri));
        }
    }

    /** Class the streams doc id's to _mget endpoint in ES */
    private static class DocIdStreamingEntity extends AbstractHttpEntity
    {
        private static final byte[] HEADER_BYTES = "{\"ids\":[".getBytes();
        private static final byte[] FOOTER_BYTES = "]}".getBytes();
        private static final byte[] QUOTE_BYTES = "\"".getBytes();
        private static final byte[] COMMA_BYTES = ",".getBytes();
        private static final Header APPLICATION_JSON = new BasicHeader("Content-Type", "application/json");

        private final IIndexPredicate indexPredicate;
        private final IExecutionContext context;
        private final Data data;

        private DocIdStreamingEntity(IIndexPredicate indexPredicate, IExecutionContext context, Data data)
        {
            this.indexPredicate = indexPredicate;
            this.context = context;
            this.data = data;
        }

        @Override
        public boolean isRepeatable()
        {
            return false;
        }

        @Override
        public boolean isChunked()
        {
            return false;
        }

        @Override
        public long getContentLength()
        {
            return -1;
        }

        @Override
        public Header getContentType()
        {
            return APPLICATION_JSON;
        }

        @Override
        public Header getContentEncoding()
        {
            return null;
        }

        @Override
        public InputStream getContent() throws IOException, UnsupportedOperationException
        {
            return null;
        }

        @Override
        public void writeTo(OutputStream outStream) throws IOException
        {
            // Retrieve the outer values from predicate
            Iterator<IOrdinalValues> values = indexPredicate.getOuterValuesIterator(context);
            try (CountingOutputStream bos = new CountingOutputStream(outStream))
            {
                bos.write(HEADER_BYTES);

                boolean first = true;
                while (values.hasNext())
                {
                    if (!first)
                    {
                        bos.write(COMMA_BYTES);
                    }
                    first = false;
                    bos.write(QUOTE_BYTES);
                    String docId = String.valueOf(values.next()
                            .getValue(0));
                    bos.write(docId.getBytes());
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
    }
}
