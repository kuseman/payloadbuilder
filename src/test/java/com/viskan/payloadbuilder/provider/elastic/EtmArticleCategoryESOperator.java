package com.viskan.payloadbuilder.provider.elastic;

import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.operator.Row;
import com.viskan.payloadbuilder.parser.ExecutionContext;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Collections.emptyIterator;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

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
class EtmArticleCategoryESOperator implements Operator
{
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    
    private static final PoolingHttpClientConnectionManager CONNECTION_MANAGER = new PoolingHttpClientConnectionManager();
    private static final CloseableHttpClient CLIENT = HttpClientBuilder
            .create()
            .setConnectionManager(CONNECTION_MANAGER)
            .disableCookieManagement()
            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
            .build();

    private final TableAlias tableAlias;
    private final Index index;
    private final String catalogAlias;

    public EtmArticleCategoryESOperator(String catalogAlias, TableAlias tableAlias, Index index)
    {
        this.catalogAlias = catalogAlias;
        this.tableAlias = tableAlias;
        this.index = index;
    }

    /* SETTINGS FIELDS */

    private final Map<String, String> TABLE_TO_SUBTYPE = ofEntries(
            entry("article", "article"),
            entry("articleAttribute", "article"));

    private final Map<String, String> DOC_ID_PATTERN_BY_TABLE = ofEntries(
            entry("article", "EtmArticleV2_%s"),
            entry("articleattribute", "EtmArticleV2_%s"),
            entry("articleprice", "EtmArticleV2_articlePrice_%s_%s"),
            entry("articlecategory", "EtmArticleV2_%s"),
            entry("articleproperty", "EtmArticleV2_%s"),
            entry("articleattributemedia", "EtmArticleV2_%s"),
            entry("attribute1", "EtmArticleV2_attribute1_%s"),
            entry("attribute2", "EtmArticleV2_attribute2_%s"),
            entry("attribute3", "EtmArticleV2_attribute3_%s"),
            entry("propertykey", "EtmArticleV2_propertyKey_%s"),
            entry("propertyvalue", "EtmArticleV2_propertyValue_%s")
            
            );

    private static final String DOC_TYPE = "EtmArticleV2";
//    String esEndpoint = "http://192.168.10.78:9200";
//        String database = "RamosLager157TestDev".toLowerCase();
//    String database = "RamosVnpTestMain".toLowerCase();
//    int comp_id = 0;
//    String indexName = database + "_c" + comp_id + "_v3";
//    String instance = database + (comp_id > 0 ? "_" + comp_id : "");

    /* END SETTINGS FIELD */

//    Map<String, List<Row>> cache =  new HashMap<>();
    
    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        String varEndpoint = (String) context.getVariable(catalogAlias + "." + EtmArticleCategoryESCatalog.ENDPOINT_KEY);
        String varIndex = (String) context.getVariable(catalogAlias + "." + EtmArticleCategoryESCatalog.INDEX_KEY);
        
        if (isBlank(varEndpoint) || isBlank(varIndex))
        {
            throw new IllegalArgumentException("Missing endpoint/index variables in scope");
        }
        
        String table = tableAlias.getTable().toString();

        if (index != null)
        {
            String mgetUrl = String.format("%s/%s/data/_mget?filter_path=docs._source,docs._id&_source_include=_timestamp,payload.%s.columns,payload.%s.rows", varEndpoint, varIndex, table, table);
            String pattern = DOC_ID_PATTERN_BY_TABLE.get(lowerCase(table));

            // ramosvnptestmain_c0_v3
            // ramosvnptestmain_c0
            String instance = varIndex.substring(0, varIndex.lastIndexOf("_"));
            int comp_id = Integer.parseInt(instance.substring(instance.lastIndexOf("_") + 2));
            instance = instance.substring(0, instance.lastIndexOf("_"));
            
            if (comp_id > 0)
            {
                instance = instance + "_" + comp_id;
            }
            
            // TODO: thread up all ids in executor and join
            DocIdStreamingEntity entity = new DocIdStreamingEntity(context.getOperatorContext(), instance, pattern);
            return getIterator(table, scrollId ->
            {
                scrollId.setValue("DUMMY");
                HttpPost post = new HttpPost(mgetUrl);
                post.setEntity(entity);
                return post;
            });
        }

        String subType = TABLE_TO_SUBTYPE.get(table);
        final String searchUrl = String.format("%s/%s/data/_search?q=_docType:%s+AND+_subType:%s&size=%d&scroll=%s",
                varEndpoint,
                varIndex,
                DOC_TYPE,
                subType,
                250,
                "2m");
        String scrollUrl = String.format("%s/_search/scroll?scroll=%s&scroll_id=", varEndpoint, "2m");

        return getIterator(table, scrollId ->
        {
            if (scrollId.getValue() == null)
            {
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
    
    private static final int TIMESTAMP_COLUMN_INDEX = -2;
    private static final int DOC_ID_COLUMN_INDEX = -3;

    private Iterator<Row> getIterator(
            String table,
            Function<MutableObject<String>, HttpUriRequest> requestSupplier)
    {
        return new Iterator<Row>()
        {
            int rowPos = 0;

            /* DEBUG FIELDS */
//            int queryCount = 0;
//            StopWatch sw = new StopWatch();
//            StopWatch sw2 = new StopWatch();
//            int docCount = 0;
//            int rowCount = 0;
//            long deserTime = 0;
//            long reqTime = 0;
            /* DEBUG FIELDS */

            //            JsonParser parser;
            MutableObject<String> scrollId = new MutableObject<>();

            Iterator<Doc> docIt;

//            String[] columns;
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
//                        if (queryCount == 0)
//                        {
//                            sw.start();
//                        }
//
//                        queryCount++;
                        HttpUriRequest request = requestSupplier.apply(scrollId);

//                        sw2.start();
                        GetResponse gr;
                        try (CloseableHttpResponse response = CLIENT.execute(request);)
                        {
                            gr = MAPPER.readValue(response.getEntity().getContent(), GetResponse.class);
                        }
                        catch (IOException e)
                        {
                            throw new RuntimeException("Error query", e);
                        }
                        
//                        sw2.stop();
//                        reqTime += sw2.getTime(TimeUnit.MILLISECONDS);
//                        sw2.reset();
                        docIt = gr.docs != null ? gr.docs.iterator() : emptyIterator();
                        continue;
                    }
                    else if (rowsIterator == null  && !docIt.hasNext())
                    {
                        return false;
                    }
                    else if (rowsIterator == null)
                    {
                        doc = docIt.next();
                        while (!doc.isValid() && docIt.hasNext())
                        {
                            doc = docIt.next();
                        }

                        if (!doc.isValid())
                        {
                            return false;
                        }

//                        docCount++;
                        Payload payload = doc.source.payload.get(table);
                        
                        if (tableAliasColumnIndices == null)
                        {
                            tableAliasColumnIndices = new int[tableAlias.getColumns().length];
                            int index = 0;
                            for (String aliasColumn : tableAlias.getColumns())
                            {
                                int columnIndex;
                                if (equalsIgnoreCase("timestamp", aliasColumn))
                                {
                                    columnIndex = TIMESTAMP_COLUMN_INDEX;
                                }
                                else if (equalsIgnoreCase("docid", aliasColumn))
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
        };
    }

    private static class GetResponse
    {
        @JsonProperty("docs")
        List<Doc> docs;
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
        return (index != null ? "index" : "scan") + " (" + tableAlias.toString() + ")";
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

        private DocIdStreamingEntity(OperatorContext context, String instance, String pattern)
        {
            this.context = context;
            this.prefixBytes = (instance + "_").getBytes();
            this.pattern = pattern;
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
            OutputStream bos = outStream;
            bos.write(HEADER_BYTES);

            Iterator<Object[]> values = context.getOuterIndexValues();

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
            bos.flush();
            bos.close();
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
