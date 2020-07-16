package com.viskan.payloadbuilder.catalog.elastic;

import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.catalog.elastic.EtmArticleCategoryESCatalog.TypeMapping;
import com.viskan.payloadbuilder.operator.AOperator;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.operator.OperatorContext.NodeData;
import com.viskan.payloadbuilder.operator.Row;
import com.viskan.payloadbuilder.parser.ExecutionContext;

import static com.viskan.payloadbuilder.DescribeUtils.CATALOG;
import static com.viskan.payloadbuilder.DescribeUtils.INDEX;
import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Collections.emptyList;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
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

/** Operator for ETM EtmArticle/CategoryV2 synchronizers */
class EtmArticleCategoryESOperator extends AOperator
{
    private static final String TIMESTAMP = "__timestamp";
    private static final String DOCID = "__docid";
    private static final int TIMESTAMP_COLUMN_INDEX = -2;
    private static final int DOC_ID_COLUMN_INDEX = -3;

    static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static final PoolingHttpClientConnectionManager CONNECTION_MANAGER = new PoolingHttpClientConnectionManager();
    static final CloseableHttpClient CLIENT = HttpClientBuilder
            .create()
            .setConnectionManager(CONNECTION_MANAGER)
            .disableCookieManagement()
            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
            .build();

    private final TypeMapping typeMapping;
    private final TableAlias tableAlias;
    private final Index index;
    private final String catalogAlias;

    public EtmArticleCategoryESOperator(
            TypeMapping typeMapping,
            int nodeId,
            String catalogAlias,
            TableAlias tableAlias,
            Index index)
    {
        super(nodeId);
        this.typeMapping = typeMapping;
        this.catalogAlias = catalogAlias;
        this.tableAlias = tableAlias;
        this.index = index;
    }

    /* SETTINGS FIELDS */
    //
    //    private final Map<String, String> TABLE_TO_SUBTYPE = ofEntries(
    //            entry("article", "article"),
    //            entry("articlename", "article"),
    //            entry("articleproperty", "article"),
    //            entry("articleattribute", "article"),
    //            entry("articleattributemedia", "article"),
    //            entry("articlecategory", "article"),
    //            entry("articleprice", "articlePrice"),
    //            entry("attribute1", "attribute1"),
    //            entry("attribute2", "attribute2"),
    //            entry("attribute3", "attribute3"));
    //
    //    private final Map<String, String> DOC_ID_PATTERN_BY_TABLE = ofEntries(
    //            entry("article", "EtmArticleV2_%s"),
    //            entry("articlename", "EtmArticleV2_%s"),
    //            entry("articleattribute", "EtmArticleV2_%s"),
    //            entry("articleprice", "EtmArticleV2_articlePrice_%s_%s"),
    //            entry("articlecategory", "EtmArticleV2_%s"),
    //            entry("articleproperty", "EtmArticleV2_%s"),
    //            entry("articleattributemedia", "EtmArticleV2_%s"),
    //            entry("attribute1", "EtmArticleV2_attribute1_%s"),
    //            entry("attribute2", "EtmArticleV2_attribute2_%s"),
    //            entry("attribute3", "EtmArticleV2_attribute3_%s"),
    //            entry("propertykey", "EtmArticleV2_propertyKey_%s"),
    //            entry("propertyvalue", "EtmArticleV2_propertyValue_%s")
    //
    //    );

    //    private static final String DOC_TYPE = "EtmArticleV2";
    //    String esEndpoint = "http://192.168.10.78:9200";
    //        String database = "RamosLager157TestDev".toLowerCase();
    //    String database = "RamosVnpTestMain".toLowerCase();
    //    int comp_id = 0;
    //    String indexName = database + "_c" + comp_id + "_v3";
    //    String instance = database + (comp_id > 0 ? "_" + comp_id : "");

    /* END SETTINGS FIELD */

    //    Map<String, List<Row>> cache =  new HashMap<>();

    @Override
    public String getName()
    {
        return (index != null ? "index" : "scan") + " (" + tableAlias.getTable() + ")";
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        Map<String, Object> result = ofEntries(true,
                entry(CATALOG, EtmArticleCategoryESCatalog.NAME));

        if (index != null)
        {
            result.put(INDEX, index);
        }
        return result;
    }

    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        String endpoint;
        String table;
        String instance;

        List<String> parts = tableAlias.getTable().getParts();

        // Three part qualified name -> <endpoint>.<instance>.<table>
        if (parts.size() == 3)
        {
            endpoint = parts.get(0);
            instance = parts.get(1);
            table = parts.get(2);
        }
        // Tow or one part qualified name -> <instance>.<table> or <table>
        else if (parts.size() <= 2)
        {
            endpoint = (String) context.getSession().getCatalogProperty(catalogAlias, EtmArticleCategoryESCatalog.ENDPOINT_KEY);
            if (isBlank(endpoint))
            {
                throw new IllegalArgumentException("Missing endpoint key in catalog properties.");
            }

            if (parts.size() == 2)
            {
                instance = parts.get(0);
                table = parts.get(1);
            }
            else
            {
                instance = (String) context.getSession().getCatalogProperty(catalogAlias, EtmArticleCategoryESCatalog.INSTANCE_KEY);
                if (isBlank(instance))
                {
                    throw new IllegalArgumentException("Missing instance key in catalog properties.");
                }
                table = parts.get(0);
            }
        }
        else
        {
            throw new IllegalArgumentException("Invalid qualified table name " + tableAlias.getTable() + ". Requires 1 to 3 parts. <endpoint>.<instance>.<table>");
        }

        String database = lowerCase(instance);
        int comp_id = 0;
        int cIndex = instance.lastIndexOf("_");
        if (cIndex != -1)
        {
            database = instance.substring(0, cIndex);
            comp_id = Integer.parseInt(instance.substring(cIndex + 1));
        }
        String indexName = database + "_c" + comp_id + "_v3";

        // TODO: turn on only in analyze mode
        NodeData nodeData = context.getOperatorContext().getNodeData(nodeId, NodeData::new);
        AtomicLong sentBytes = (AtomicLong) nodeData.properties.computeIfAbsent("sentCount", k -> new AtomicLong());
        AtomicLong receivedBytes = (AtomicLong) nodeData.properties.computeIfAbsent("receivedCount", k -> new AtomicLong());
        AtomicInteger scrollCount = (AtomicInteger) nodeData.properties.computeIfAbsent("scrollCount", k -> new AtomicInteger());

        if (index != null)
        {
            String mgetUrl = String.format("%s/%s/data/_mget?filter_path=docs._source,docs._id&_source_include=_timestamp,payload.%s.columns,payload.%s.rows", endpoint, indexName, table, table);
            if (isBlank(typeMapping.docIdPattern))
            {
                throw new RuntimeException("No doc id pattern registered for " + table);
            }
            // TODO: thread up all ids in executor and join
            DocIdStreamingEntity entity = new DocIdStreamingEntity(context.getOperatorContext(), instance, typeMapping.docIdPattern, sentBytes);
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
                "%s/%s/data/_search?q=_docType:%s+AND+_subType:%s&size=%d&scroll=%s&filter_path=_scroll_id,hits.hits._id,hits.hits._source&_source_include=_timestamp,payload.%s.columns,payload.%s.rows",
                endpoint,
                indexName,
                typeMapping.doctype,
                typeMapping.subtype,
                1000,
                "2m",
                typeMapping.name,
                typeMapping.name);
        String scrollUrl = String.format("%s/_search/scroll?scroll=%s&scroll_id=", endpoint, "2m");

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
            int[] tableAliasColumnIndices;
            Doc doc;
            Iterator<Object[]> rowsIterator;

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
                    else if (rowsIterator == null)
                    {
                        if (!docIt.hasNext())
                        {
                            docIt = null;
                            continue;
                        }

                        doc = docIt.next();
                        while (!doc.isValid() && docIt.hasNext())
                        {
                            doc = docIt.next();
                        }

                        if (!doc.isValid())
                        {
                            docIt = null;
                            continue;
                        }

                        Payload payload = doc.source.payload.get(typeMapping.name);
                        if (tableAliasColumnIndices == null)
                        {
                            populateColumnIndices(payload);
                        }

                        rowsIterator = payload.rows.iterator();
                        continue;
                    }
                    else if (!rowsIterator.hasNext())
                    {
                        rowsIterator = null;
                        continue;
                    }

                    Object[] data = rowsIterator.next();
                    Object[] rowData = new Object[tableAliasColumnIndices.length];

                    for (int i = 0; i < rowData.length; i++)
                    {
                        int columnIndex = tableAliasColumnIndices[i];
                        if (columnIndex == TIMESTAMP_COLUMN_INDEX)
                        {
                            rowData[i] = doc.source.timestamp;
                        }
                        else if (columnIndex == DOC_ID_COLUMN_INDEX)
                        {
                            rowData[i] = doc.docId;
                        }
                        else
                        {
                            rowData[i] = columnIndex >= 0 && columnIndex < data.length ? data[columnIndex] : null;
                        }
                    }

                    next = Row.of(tableAlias, rowPos++, rowData);
                }
                return true;
            }

            private void populateColumnIndices(Payload payload)
            {
                // All columns wanted, update alias columns
                if (tableAlias.getColumns() == null)
                {
                    int length = payload.columns.length;
                    String[] columns = Arrays.copyOf(payload.columns, length + 2);
                    columns[length] = TIMESTAMP;
                    columns[length + 1] = DOCID;

                    tableAliasColumnIndices = new int[columns.length];
                    tableAlias.setColumns(columns);
                }
                else
                {
                    tableAliasColumnIndices = new int[tableAlias.getColumns().length];
                }

                int index = 0;
                for (String aliasColumn : tableAlias.getColumns())
                {
                    int columnIndex;
                    if (equalsIgnoreCase(TIMESTAMP, aliasColumn))
                    {
                        columnIndex = TIMESTAMP_COLUMN_INDEX;
                    }
                    else if (equalsIgnoreCase(DOCID, aliasColumn))
                    {
                        columnIndex = DOC_ID_COLUMN_INDEX;
                    }
                    else
                    {
                        columnIndex = ArrayUtils.indexOf(payload.columns, aliasColumn);
                    }
                    tableAliasColumnIndices[index++] = columnIndex;
                }
            }
        };

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
        Source source;

        @JsonProperty("_id")
        String docId;

        public boolean isValid()
        {
            return source != null && source.payload != null;
        }
    }

    private static class Source
    {
        @JsonProperty
        Map<String, Payload> payload;

        @JsonProperty("_timestamp")
        String timestamp;
    }

    private static class Payload
    {
        @JsonProperty
        String[] columns;
        @JsonProperty
        List<Object[]> rows;
    }

    @Override
    public String toString()
    {
        return String.format("ID: %d, %s", nodeId, (index != null ? "index" : "scan") + " (" + tableAlias.toString() + ")");
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
        private final String pattern;
        private final byte[] prefixBytes;
        private final AtomicLong sentBytes;

        private DocIdStreamingEntity(OperatorContext context, String instance, String pattern, AtomicLong sentBytes)
        {
            this.context = context;
            this.prefixBytes = (instance + "_").getBytes();
            this.pattern = pattern;
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
                    bos.write(prefixBytes);
                    String docId = String.format(pattern, values.next());
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
