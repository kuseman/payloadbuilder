package org.kuse.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.kuse.payloadbuilder.catalog.es.ESCatalog.SINGLE_TYPE_TABLE_NAME;
import static org.kuse.payloadbuilder.core.DescribeUtils.CATALOG;
import static org.kuse.payloadbuilder.core.DescribeUtils.PREDICATE;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
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
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;
import org.kuse.payloadbuilder.catalog.es.ESCatalog.IdIndex;
import org.kuse.payloadbuilder.catalog.es.ESCatalog.ParentIndex;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.catalog.TableMeta;
import org.kuse.payloadbuilder.core.catalog.TableMeta.Column;
import org.kuse.payloadbuilder.core.operator.AOperator;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.IIndexValuesFactory.IIndexValues;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.StatementContext;
import org.kuse.payloadbuilder.core.operator.StatementContext.NodeData;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.utils.ObjectUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

/** Operator for Elastic search */
class ESOperator extends AOperator
{
    private static final int DEFAULT_RECIEVE_TIMEOUT = 15000;
    private static final int DEFAULT_CONNECT_TIMEOUT = 500;
    static final Charset UTF_8 = Charset.forName("UTF-8");
    private static final String FIELDS = "fields";
    static final String INDEX = "__index";
    static final String TYPE = "__type";
    static final String DOCID = "__id";
    static final String PARENTID = "__parent_id";
    static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private static final ObjectReader READER = MAPPER.readerFor(ESResponse.class);

    private static final PoolingHttpClientConnectionManager CONNECTION_MANAGER = new PoolingHttpClientConnectionManager();
    static final CloseableHttpClient CLIENT = HttpClientBuilder
            .create()
            .setDefaultHeaders(asList(
                    new BasicHeader(HttpHeaders.ACCEPT, "application/json"),
                    new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json")))
            .setConnectionManager(CONNECTION_MANAGER)
            .disableCookieManagement()
            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
            .setDefaultRequestConfig(RequestConfig
                    .custom()
                    .setContentCompressionEnabled(true)
                    .setConnectTimeout(DEFAULT_CONNECT_TIMEOUT)
                    .setSocketTimeout(DEFAULT_RECIEVE_TIMEOUT)
                    .build())
            .build();

    private final TableAlias tableAlias;
    private final Index index;
    private final String catalogAlias;
    private final List<PropertyPredicate> propertyPredicates;
    private final List<Pair<String, String>> sortItems;

    ESOperator(
            int nodeId,
            String catalogAlias,
            TableAlias tableAlias,
            Index index,
            List<PropertyPredicate> propertyPredicates,
            List<Pair<String, String>> sortItems)
    {
        super(nodeId);
        this.catalogAlias = catalogAlias;
        this.tableAlias = tableAlias;
        this.index = index;
        this.propertyPredicates = requireNonNull(propertyPredicates, "propertyPredicates");
        this.sortItems = requireNonNull(sortItems, "sortItems");
    }

    @Override
    public String getName()
    {
        return (index != null ? "index" : "scan") + " (" + tableAlias.getTable() + ")";
    }

    @Override
    public Map<String, Object> getDescribeProperties(ExecutionContext context)
    {
        ESType esType = ESType.of(context.getSession(), catalogAlias, tableAlias.getTable());

        Map<String, Object> result = ofEntries(true,
                entry(CATALOG, ESCatalog.NAME),
                entry(PREDICATE, propertyPredicates
                        .stream()
                        .map(PropertyPredicate::getDescription)
                        .collect(joining(" AND "))),
                entry("Sort", sortItems
                        .stream()
                        .map(i -> i.getKey() + ":" + i.getValue())
                        .collect(joining(","))),
                entry("Query", ESUtils.getSearchBody(sortItems, propertyPredicates, SINGLE_TYPE_TABLE_NAME.equals(esType.type), context)));

        if (index != null)
        {
            result.put(INDEX, index);
        }

        Data data = context.getStatementContext().getNodeData(nodeId);
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
    public RowIterator open(ExecutionContext context)
    {
        ESType esType = ESType.of(context.getSession(), catalogAlias, tableAlias.getTable());

        Data data = context.getStatementContext().getOrCreateNodeData(nodeId, Data::new);
        if (index instanceof IdIndex)
        {
            boolean isSingleType = SINGLE_TYPE_TABLE_NAME.equals(esType.type);
            String mgetUrl = getUrl(
                    String.format("%s/%s/%s/_mget?", esType.endpoint, esType.index, esType.type),
                    "docs",
                    tableAlias,
                    index,
                    isSingleType);

            DocIdStreamingEntity entity = new DocIdStreamingEntity(context.getStatementContext(), data);
            MutableBoolean doRequest = new MutableBoolean(true);
            return getIterator(
                    context,
                    tableAlias,
                    esType.endpoint,
                    ESCatalog.SINGLE_TYPE_TABLE_NAME.equals(esType.type),
                    data,
                    scrollId ->
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

        final boolean isSingleType = ESCatalog.SINGLE_TYPE_TABLE_NAME.equals(esType.type);
        final String searchUrl = getSearchUrl(
                esType.endpoint,
                esType.index,
                esType.type,
                1000,
                2,
                tableAlias,
                index);
        final String scrollUrl = getScrollUrl(
                esType.endpoint,
                esType.type,
                2,
                tableAlias,
                index);

        String body;
        // Parent index, query parents
        if (index instanceof ParentIndex)
        {
            List<String> parentIds = new ArrayList<>();
            while (context.getStatementContext().getOuterIndexValues().hasNext())
            {
                IIndexValues array = context.getStatementContext().getOuterIndexValues().next();
                Object obj = array.getValue(0);
                if (obj != null)
                {
                    parentIds.add(String.valueOf(obj));
                }
            }

            body = "{ \"filter\": { \"terms\": { \"_parent\": [ " + parentIds.stream().collect(joining(",")) + " ] } } }";
        }
        else
        {
            body = ESUtils.getSearchBody(sortItems, propertyPredicates, isSingleType, context);
        }

        final String actualBody = body;
        return getIterator(
                context,
                tableAlias,
                esType.endpoint,
                isSingleType,
                data,
                scrollId ->
                {
                    if (scrollId.getValue() == null)
                    {
                        data.bytesSent += searchUrl.length() + actualBody.length();
                        HttpPost post = new HttpPost(searchUrl);
                        post.setEntity(new StringEntity(actualBody, UTF_8));
                        return post;
                    }
                    else
                    {
                        String id = scrollId.getValue();
                        data.bytesSent += scrollUrl.length() + actualBody.length();
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

    static String getSearchUrl(
            String endpoint,
            String index,
            String type,
            Integer size,
            Integer scrollMinutes,
            TableAlias alias,
            Index operatorIndex)
    {
        boolean isSingleType = ESCatalog.SINGLE_TYPE_TABLE_NAME.equals(type);
        ObjectUtils.requireNonBlank(endpoint, "endpoint is required");
        return getUrl(String.format("%s/%s%s/_search?%s%s",
                endpoint,
                isBlank(index) ? "*" : index,
                isBlank(type) ? "" : ("/" + type),
                scrollMinutes != null ? ("scroll=" + scrollMinutes + "m") : "",
                size != null ? ("&size=" + size) : ""), "hits.hits", alias, operatorIndex, isSingleType);
    }

    static String getSearchTemplateUrl(
            String endpoint,
            String index,
            String type,
            Integer size,
            Integer scrollMinutes,
            TableAlias alias,
            Index operatorIndex)
    {
        boolean isSingleType = ESCatalog.SINGLE_TYPE_TABLE_NAME.equals(type);
        ObjectUtils.requireNonBlank(endpoint, "endpoint is required");
        return getUrl(String.format("%s/%s/%s_search/template?%s",
                endpoint,
                isBlank(index) ? "*" : index,
                isBlank(type) ? "" : (type + "/"),
                scrollMinutes != null ? ("scroll=" + scrollMinutes + "m") : "",
                size != null ? ("&size=" + size) : ""), "hits.hits", alias, operatorIndex, isSingleType);
    }

    static String getScrollUrl(
            String endpoint,
            String type,
            int scrollMinutes,
            TableAlias alias,
            Index operatorIndex)
    {
        boolean isSingleType = ESCatalog.SINGLE_TYPE_TABLE_NAME.equals(type);
        ObjectUtils.requireNonBlank(endpoint, "endpoint is required");
        return getUrl(String.format("%s/_search/scroll?scroll=%dm",
                endpoint,
                scrollMinutes), "hits.hits", alias, operatorIndex, isSingleType);
    }

    private static String getUrl(
            String prefix,
            String filterPathPrefix,
            TableAlias alias,
            Index operatorIndex,
            boolean isSingleType)
    {
        TableMeta meta = alias.getTableMeta();
        boolean includeParent = !isSingleType
            && (meta == null
                || meta.getColumn(PARENTID) != null
                || operatorIndex instanceof ParentIndex);
        boolean includeIndex = meta == null
            || meta.getColumn(INDEX) != null;
        boolean includeType = meta == null
            || meta.getColumn(TYPE) != null;
        boolean includeId = meta == null
            || meta.getColumn(DOCID) != null;

        String filterPath = getFilterPath(filterPathPrefix, includeParent, includeIndex, includeType, includeId);
        String fields = getFields(includeParent);
        return prefix + filterPath
            + (!isBlank(fields) ? ("&" + fields) : "");
    }

    private static String getFields(boolean includeParent)
    {
        // TODO: version 5 and above then fields is called stored_fields
        return includeParent ? "fields=_source,_parent" : "";
    }

    private static String getFilterPath(
            String filterPathPrefix,
            boolean includeParent,
            boolean includeIndex,
            boolean includeType,
            boolean includeId)
    {
        StringBuilder sb = new StringBuilder("&filter_path=_scroll_id,")
                .append(filterPathPrefix)
                .append("._source");
        if (includeParent)
        {
            sb.append(",").append(filterPathPrefix).append(".fields");
        }
        if (includeIndex)
        {
            sb.append(",").append(filterPathPrefix).append("._index");
        }
        if (includeType)
        {
            sb.append(",").append(filterPathPrefix).append("._type");
        }
        if (includeId)
        {
            sb.append(",").append(filterPathPrefix).append("._id");
        }
        return sb.toString();
    }

    //CSOFF
    static RowIterator getIterator(
            //CSON
            ExecutionContext context,
            TableAlias tableAlias,
            String endpoint,
            boolean isSingleType,
            Data data,
            Function<MutableObject<String>, HttpUriRequest> requestSupplier)
    {
        //CSOFF
        return new RowIterator()
        //CSON
        {
            private final Set<String> metaColumns = tableAlias.getTableMeta() == null
                ? emptySet()
                : tableAlias.getTableMeta().getColumns().stream().map(c -> c.getName()).collect(toSet());
            private Set<String> addedColumns;
            private String[] rowColumns;
            private int rowPos;
            private final MutableObject<String> scrollId = new MutableObject<>();
            private Iterator<Doc> docIt;
            private Row next;
            private boolean closed;

            @Override
            public boolean hasNext()
            {
                return !closed && setNext();
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

                    try (CloseableHttpResponse response = CLIENT.execute(delete))
                    {
                        int status = response.getStatusLine().getStatusCode();
                        if (!(status == HttpStatus.SC_OK || status == HttpStatus.SC_NOT_FOUND))
                        {
                            String body = IOUtils.toString(response.getEntity().getContent());
                            throw new RuntimeException("Error clearing scroll: " + body);
                        }
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException("Error deleting scroll", e);
                    }
                }
            }

            //CSOFF
            private boolean setNext()
            //CSON
            {
                while (next == null)
                {
                    if (context.getSession().abortQuery())
                    {
                        return false;
                    }

                    if (docIt == null)
                    {
                        HttpUriRequest request = requestSupplier.apply(scrollId);

                        if (request == null)
                        {
                            return false;
                        }

                        ESResponse esReponse;
                        data.requestCount++;
                        data.requestTime.resume();
                        try (CloseableHttpResponse response = CLIENT.execute(request);)
                        {
                            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK)
                            {
                                throw new RuntimeException("Error query Elastic. " + IOUtils.toString(response.getEntity().getContent()));
                            }
                            CountingInputStream cis = new CountingInputStream(response.getEntity().getContent());
                            esReponse = READER.withAttribute(FIELDS, metaColumns).readValue(cis);
                            data.bytesReceived += cis.getByteCount();
                        }
                        catch (IOException e)
                        {
                            throw new RuntimeException("Error query", e);
                        }
                        finally
                        {
                            data.requestTime.suspend();
                        }

                        scrollId.setValue(esReponse.scrollId);
                        docIt = esReponse.getDocs().iterator();

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

                    if (tableAlias.getTableMeta() == null)
                    {
                        if (addedColumns == null)
                        {
                            Set<String> keySet = doc.source.keySet();
                            //CSOFF
                            addedColumns = new LinkedHashSet<>(keySet.size() + 4);
                            //CSON
                            addedColumns.add(INDEX);
                            addedColumns.add(TYPE);
                            addedColumns.add(DOCID);
                            addedColumns.add(PARENTID);
                            addedColumns.addAll(keySet);
                            rowColumns = addedColumns.toArray(EMPTY_STRING_ARRAY);
                        }
                        else if (addedColumns.addAll(doc.source.keySet()))
                        {
                            rowColumns = addedColumns.toArray(EMPTY_STRING_ARRAY);
                        }
                        next = Row.of(tableAlias, rowPos++, rowColumns, new DocValues(doc, rowColumns, tableAlias));
                    }
                    else
                    {
                        next = Row.of(tableAlias, rowPos++, new DocValues(doc, null, tableAlias));
                    }
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
        private final TableAlias alias;

        DocValues(Doc doc, String[] columns, TableAlias alias)
        {
            this.doc = doc;
            this.columns = columns;
            this.alias = alias;
        }

        @Override
        public Object getValue(int ordinal)
        {
            if (ordinal == -1)
            {
                return null;
            }
            // Asterisk column then meta fields are located in the first ordinals
            if (alias.getTableMeta() == null)
            {
                if (ordinal == 0)
                {
                    return doc.index;
                }
                else if (ordinal == 1)
                {
                    return doc.type;
                }
                else if (ordinal == 2)
                {
                    return doc.docId;
                }
                else if (ordinal == 3)
                {
                    return doc.fields != null ? doc.fields.get("_parent") : null;
                }
                return doc.source.get(columns[ordinal]);
            }

            Column column = alias.getTableMeta().getColumn(ordinal);
            if (column == null)
            {
                return null;
            }
            String col = column.getName();
            if (PARENTID.equals(col))
            {
                return doc.fields.get("_parent");
            }
            else if (INDEX.equals(col))
            {
                return doc.index;
            }
            else if (TYPE.equals(col))
            {
                return doc.type;
            }
            return doc.source.get(col);
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

    /** Doc */
    private static class Doc
    {
        @JsonDeserialize(using = SourceDeserializer.class)
        @JsonProperty("_source")
        Map<String, Object> source;
        @JsonProperty("fields")
        Map<String, Object> fields = emptyMap();
        @JsonProperty("_index")
        String index;
        @JsonProperty("_type")
        String type;
        @JsonProperty("_id")
        String docId;
        @JsonProperty("found")
        Boolean found;

        public boolean isValid()
        {
            // mget => found
            // search =>
            return source != null && (found == null || found);
        }
    }

    /** Custom deserializer for _source field that only picks wanted fields */
    private static class SourceDeserializer extends JsonDeserializer<Map<String, Object>>
    {
        @Override
        public Map<String, Object> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException
        {
            @SuppressWarnings("unchecked")
            Set<String> fields = (Set<String>) ctxt.getAttribute(FIELDS);
            Map<String, Object> result = new HashMap<>();
            p.nextToken();  // START_OBJECT
            while (p.currentToken() != JsonToken.END_OBJECT)
            {
                String name = p.currentName();
                if (fields.isEmpty() || fields.contains(name))
                {
                    p.nextToken();
                    result.put(name, p.readValueAs(Object.class));
                }
                else
                {
                    p.skipChildren();
                }
                p.nextToken();
            }
            return result;
        }
    }

    @Override
    public String toString()
    {
        return String.format("ID: %d, %s", nodeId, (index != null ? "index" : "scan") + " (" + tableAlias.toString() + ")");
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
    private static class DocIdStreamingEntity implements HttpEntity
    {
        private static final byte[] HEADER_BYTES = "{\"ids\":[".getBytes();
        private static final byte[] FOOTER_BYTES = "]}".getBytes();
        private static final byte[] QUOTE_BYTES = "\"".getBytes();
        private static final byte[] COMMA_BYTES = ",".getBytes();
        private static final Header APPLICATION_JSON = new BasicHeader("Content-Type", "application/json");

        private final StatementContext context;
        private final Data data;

        private DocIdStreamingEntity(StatementContext context, Data data)
        {
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
            Iterator<IIndexValues> values = context.getOuterIndexValues();
            try (CountingOutputStream bos = new CountingOutputStream(outStream))
            {
                bos.write(HEADER_BYTES);

                boolean first = true;
                while (values.hasNext())
                {
                    String docId = String.valueOf(values.next().getValue(0));
                    if (!first)
                    {
                        bos.write(COMMA_BYTES);
                    }
                    first = false;
                    bos.write(QUOTE_BYTES);
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

        @Override
        public void consumeContent() throws IOException
        {
        }
    }
}
