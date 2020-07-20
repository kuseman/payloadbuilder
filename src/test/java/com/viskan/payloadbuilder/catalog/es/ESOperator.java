package com.viskan.payloadbuilder.catalog.es;

import com.viskan.payloadbuilder.QuerySession;
import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.AOperator;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.operator.OperatorContext.NodeData;
import com.viskan.payloadbuilder.operator.Row;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.Expression;
import com.viskan.payloadbuilder.parser.QualifiedName;

import static com.viskan.payloadbuilder.DescribeUtils.CATALOG;
import static com.viskan.payloadbuilder.DescribeUtils.INDEX;
import static com.viskan.payloadbuilder.DescribeUtils.PREDICATE;
import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

import org.apache.commons.io.input.CountingInputStream;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeader;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Operator for Elastic search */
class ESOperator extends AOperator
{
    static final String DOCID = "__id";
    static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final PoolingHttpClientConnectionManager CONNECTION_MANAGER = new PoolingHttpClientConnectionManager();
    static final CloseableHttpClient CLIENT = HttpClientBuilder
            .create()
            .setConnectionManager(CONNECTION_MANAGER)
            .disableCookieManagement()
            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
            .build();

    private final TableAlias tableAlias;
    private final Index index;
    private final String catalogAlias;
    private final List<Pair<String, Expression>> fieldPredicates;
    
    public ESOperator(
            int nodeId,
            String catalogAlias,
            TableAlias tableAlias,
            Index index,
            List<Pair<String, Expression>> fieldPredicates)
    {
        super(nodeId);
        this.catalogAlias = catalogAlias;
        this.tableAlias = tableAlias;
        this.index = index;
        this.fieldPredicates = requireNonNull(fieldPredicates, "fieldPredicates");
    }

    @Override
    public String getName()
    {
        return (index != null ? "index" : "scan") + " (" + tableAlias.getTable() + ")";
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        Map<String, Object> result = ofEntries(true,
                entry(CATALOG, ESCatalog.NAME),
                entry(PREDICATE, fieldPredicates.stream().map(p -> p.getKey() + " = " + p.getValue()).collect(joining(" AND "))));

        if (index != null)
        {
            result.put(INDEX, index);
        }
        return result;
    }
    
    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        EsType esType = EsType.of(context.getSession(), catalogAlias, tableAlias.getTable());

        // TODO: turn on only in analyze mode
        NodeData nodeData = context.getOperatorContext().getNodeData(nodeId, NodeData::new);
        AtomicLong sentBytes = (AtomicLong) nodeData.properties.computeIfAbsent("sentCount", k -> new AtomicLong());
        AtomicLong receivedBytes = (AtomicLong) nodeData.properties.computeIfAbsent("receivedCount", k -> new AtomicLong());
        AtomicInteger scrollCount = (AtomicInteger) nodeData.properties.computeIfAbsent("scrollCount", k -> new AtomicInteger());

        if (index != null)
        {
            String mgetUrl = String.format("%s/%s/%s/_mget?filter_path=docs._source,docs._id", esType.endpoint, esType.index, esType.type);
            // TODO: thread up all ids in executor and join
            DocIdStreamingEntity entity = new DocIdStreamingEntity(context.getOperatorContext(), sentBytes);
            return getIterator(
                    receivedBytes,
                    scrollId ->
                    {
                        HttpPost post = new HttpPost(mgetUrl);
                        post.setEntity(entity);
                        return post;
                    });
        }

        final String searchUrl = String.format(
                "%s/%s/%s/_search?%s&size=%d&scroll=%s&filter_path=_scroll_id,hits.hits._id,hits.hits._source",
                esType.endpoint,
                esType.index,
                esType.type,
                getPredicateParam(context),
                // TODO: table option for batch size
                1000,
                "2m");
        String scrollUrl = String.format("%s/_search/scroll?scroll=%s&scroll_id=", esType.endpoint, "2m");

        return getIterator(
                receivedBytes,
                scrollId ->
                {
                    scrollCount.incrementAndGet();
                    if (scrollId.getValue() == null)
                    {
                        sentBytes.addAndGet(searchUrl.length());
                        return new HttpGet(searchUrl);
                    }
                    else
                    {
                        String id = scrollId.getValue();
                        scrollId.setValue(null);
                        return new HttpGet(scrollUrl + id);
                    }
                });
    }

    private Iterator<Row> getIterator(
            AtomicLong receivedBytes,
            Function<MutableObject<String>, HttpUriRequest> requestSupplier)
    {
        return new Iterator<Row>()
        {
            int rowPos = 0;
            MutableObject<String> scrollId = new MutableObject<>();
            Iterator<Doc> docIt;
            Row next;

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            @Override
            public Row next()
            {
                Row result = next;
                next = null;
                return result;
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    if (docIt == null)
                    {
                        HttpUriRequest request = requestSupplier.apply(scrollId);
                        ESResponse esReponse;
                        try (CloseableHttpResponse response = CLIENT.execute(request);)
                        {
                            CountingInputStream cis = new CountingInputStream(response.getEntity().getContent());
                            esReponse = MAPPER.readValue(cis, ESResponse.class);
                            receivedBytes.addAndGet(cis.getByteCount());
                        }
                        catch (IOException e)
                        {
                            throw new RuntimeException("Error query", e);
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

                    if (tableAlias.getColumns() == null && !doc.source.keySet().isEmpty())
                    {
                        List<String> columns = new ArrayList<>(doc.source.keySet());
                        columns.add(DOCID);
                        tableAlias.setColumns(columns.toArray(EMPTY_STRING_ARRAY));
                    }
                    
                    Object[] data;
                    if (tableAlias.getColumns() != null)
                    {
                        data = new Object[tableAlias.getColumns().length];
                        int index = 0;
                        for (String column : tableAlias.getColumns())
                        {
                            if (DOCID.equals(column))
                            {
                                data[index++] = doc.docId;
                            }
                            else
                            {
                                data[index++] = doc.source.get(column);
                            }
                        }
                    }
                    else
                    {
                        data = ArrayUtils.EMPTY_OBJECT_ARRAY;
                    }

                    next = Row.of(tableAlias, rowPos++, data);
                }
                return true;
            }
        };
    }

    private String getPredicateParam(ExecutionContext context)
    {
        if (fieldPredicates.isEmpty())
        {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (Pair<String, Expression> pair : fieldPredicates)
        {
            if (sb.length() == 0)
            {
                sb.append("q=");
            }
            else if (sb.length() > 0)
            {
                sb.append("+AND+");
            }
            
            Object value = pair.getValue().eval(context);
            sb.append(pair.getKey()).append(":").append(value);
        }
        
        return sb.toString();
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

    private static class OuterHits
    {
        @JsonProperty("hits")
        List<Doc> hits;
    }

    private static class Doc
    {
        @JsonProperty("_source")
        Map<String, Object> source;

        @JsonProperty("_id")
        String docId;

        public boolean isValid()
        {
            return source != null;
        }
    }

    @Override
    public String toString()
    {
        return String.format("ID: %d, %s", nodeId, (index != null ? "index" : "scan") + " (" + tableAlias.toString() + ")");
    }
    
    /** Class representing a endpoint/index/type combo. */
    static class EsType
    {
        final String endpoint;
        final String index;
        final String type;
        
        EsType(String endpoint, String index, String type)
        {
            this.endpoint = endpoint;
            this.index = index;
            this.type = type;
        }
        
        /** Create type from provided session/table */
        static EsType of(QuerySession session, String catalogAlias, QualifiedName table)
        {
            String endpoint;
            String indexName;
            String type;

            List<String> parts = table.getParts();

            // Three part qualified name -> <endpoint>.<index>.<type>
            if (parts.size() == 3)
            {
                endpoint = parts.get(0);
                indexName = parts.get(1);
                type = parts.get(2);
            }
            // Tow or one part qualified name -> <index>.<type> or <type>
            else if (parts.size() <= 2)
            {
                endpoint = (String) session.getCatalogProperty(catalogAlias, ESCatalog.ENDPOINT_KEY);
                if (isBlank(endpoint))
                {
                    throw new IllegalArgumentException("Missing endpoint key in catalog properties.");
                }

                if (parts.size() == 2)
                {
                    indexName = parts.get(0);
                    type = parts.get(1);
                }
                else
                {
                    indexName = (String) session.getCatalogProperty(catalogAlias, ESCatalog.INDEX_KEY);
                    if (isBlank(indexName))
                    {
                        throw new IllegalArgumentException("Missing index key in catalog properties.");
                    }
                    type = parts.get(0);
                }
            }
            else
            {
                throw new IllegalArgumentException("Invalid qualified table name " + table + ". Requires 1 to 3 parts. <endpoint>.<index>.<type>");
            }
            
            return new EsType(endpoint, indexName, type);
        }
    }

    /** Class the streams doc id's to _mget endpoint in ES */
    private static class DocIdStreamingEntity implements HttpEntity
    {
        private static final byte[] HEADER_BYTES = "{ \"ids\": [".getBytes();
        private static final byte[] FOOTER_BYTES = "]}".getBytes();
        private static final byte[] QUOTE_BYTES = "\"".getBytes();
        private static final byte[] COMMA_BYTES = ",".getBytes();
        private static final Header APPLICATION_JSON = new BasicHeader("Content-Type", "application/json");

        private final OperatorContext context;
        private final AtomicLong sentBytes;

        private DocIdStreamingEntity(OperatorContext context, AtomicLong sentBytes)
        {
            this.context = context;
            this.sentBytes = sentBytes;
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
            Iterator<Object[]> values = context.getOuterIndexValues();
            if (values == null || !values.hasNext())
            {
                return;
            }

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
                    String docId = String.valueOf(values.next()[0]);
                    bos.write(docId.getBytes());
                    bos.write(QUOTE_BYTES);
                }

                bos.write(FOOTER_BYTES);
                sentBytes.addAndGet(bos.getByteCount());
            }
        }

        @Override
        public boolean isStreaming()
        {
            return true;
        }

        @Override
        public void consumeContent() throws IOException
        {
        }
    }
}
