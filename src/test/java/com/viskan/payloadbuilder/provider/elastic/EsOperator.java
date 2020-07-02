package com.viskan.payloadbuilder.provider.elastic;

import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.operator.Row;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;
import static java.util.Collections.emptyIterator;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.time.StopWatch;
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

class EsOperator implements Operator
{
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//    private static final TypeReference<List<Object[]>> LIST_OF_OBJECT_ARRAY = new TypeReference<List<Object[]>>()
//    {
//    };
    
    private static final PoolingHttpClientConnectionManager CONNECTION_MANAGER = new PoolingHttpClientConnectionManager();
    private static final CloseableHttpClient CLIENT = HttpClientBuilder
            .create()
            .setConnectionManager(CONNECTION_MANAGER)
            .disableCookieManagement()
            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
            .build();

    private final TableAlias tableAlias;
    private final Index index;

    public EsOperator(TableAlias tableAlias, Index index)
    {
        this.tableAlias = tableAlias;
        this.index = index;
    }

    /* SETTINGS FIELDS */

    private final Map<String, String> TABLE_TO_SUBTYPE = ofEntries(
            entry("article", "article"),
            entry("articleAttribute", "article"));

    private final Map<String, String> DOC_ID_PATTERN_BY_TABLE = ofEntries(
            entry("article", "EtmArticleV2_%s"),
            entry("articleAttribute", "EtmArticleV2_%s"),
            entry("articleCategory", "EtmArticleV2_%s"),
            entry("articleProperty", "EtmArticleV2_%s"),
            entry("articleAttributeMedia", "EtmArticleV2_%s"));

    String docType = "EtmArticleV2";
    String esEndpoint = "http://192.168.10.78:9200";
//        String database = "RamosLager157TestDev".toLowerCase();
    String database = "RamosVnpTestMain".toLowerCase();
    int comp_id = 0;
    String indexName = database + "_c" + comp_id + "_v3";
    String instance = database + (comp_id > 0 ? "_" + comp_id : "");

    /* END SETTINGS FIELD */

//    Map<String, List<Row>> cache =  new HashMap<>();
    
    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        String table = tableAlias.getTable().toString();

        if (index != null)
        {
            String mgetUrl = String.format("%s/%s/data/_mget?filter_path=docs._source&_source_include=payload.%s.columns,payload.%s.rows", esEndpoint, indexName, table, table);
            String pattern = DOC_ID_PATTERN_BY_TABLE.get(table);

            // TODO: thread up all ids in executor and join
            DocIdStreamingEntity entity = new DocIdStreamingEntity(context, instance, pattern);
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
                esEndpoint,
                indexName,
                docType,
                subType,
                250,
                "2m");
        String scrollUrl = String.format("%s/_search/scroll?scroll=%s&scroll_id=", esEndpoint, "2m");

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

    private Iterator<Row> getIterator(
            String table,
            Function<MutableObject<String>, HttpUriRequest> requestSupplier)
    {
        return new Iterator<Row>()
        {
            int rowPos = 0;

            /* DEBUG FIELDS */
            int queryCount = 0;
            StopWatch sw = new StopWatch();
            StopWatch sw2 = new StopWatch();
            int docCount = 0;
            int rowCount = 0;
            long deserTime = 0;
            long reqTime = 0;
            /* DEBUG FIELDS */

            //            JsonParser parser;
            MutableObject<String> scrollId = new MutableObject<>();

            Iterator<Doc> docIt;

//            String[] columns;
            int[] tableAliasColumnIndices;
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
                    //                    try
                    //                    {
                    if (docIt == null)
                    {
                        if (queryCount == 0)
                        {
                            sw.start();
                        }

                        queryCount++;
                        HttpUriRequest request = requestSupplier.apply(scrollId);

                        sw2.start();
                        GetResponse gr;
                        try (CloseableHttpResponse response = CLIENT.execute(request);)
                        {
                            gr = MAPPER.readValue(response.getEntity().getContent(), GetResponse.class);
                        }
                        catch (IOException e)
                        {
                            throw new RuntimeException("Error query", e);
                        }
                        sw2.stop();
                        reqTime += sw2.getTime(TimeUnit.MILLISECONDS);
                        sw2.reset();

                        //                            client = BUILDER.build();

                        //                            response = client.execute(request);

                        //                            GetResponse gr = MAPPER.readValue(response.getEntity().getContent(), GetResponse.class);
                        
                        docIt = gr.docs != null ? gr.docs.iterator() : emptyIterator();

                        //                            response.close();
                        //                            client.close();

                        //                            System.out.println();

                        //                            parser = MAPPER.getFactory().createParser(response.getEntity().getContent());

                        //                            System.out.println(parser.readValueAs(Object.class));

                        continue;
                    }
                    else if (!docIt.hasNext())
                    {
//                        System.out.println("Stats for " + table + " quueryCount: " + queryCount + ", time: " + sw.toString() + ", docCount: " + docCount + ", rowCount: " + rowCount + ", deSerTime: " + DurationFormatUtils.formatDurationHMS(deserTime) + ", request time: " + DurationFormatUtils.formatDurationHMS(reqTime));
                        return false;

                        //                            client.close();
                        //                            response.close();
                        //                            parser.close();
                        //                            parser = null;
                        //                            rowsIterator = null;
                        //                            continue;
                    }
                    else if (rowsIterator == null)
                    {
                        Doc doc = docIt.next();
                        while (!doc.isValid() && docIt.hasNext())
                        {
                            doc = docIt.next();
                        }

                        if (!doc.isValid())
                        {
//                            System.out.println("Stats for " + table + " quueryCount: " + queryCount + ", time: " + sw.toString() + ", docCount: " + docCount + ", rowCount: " + rowCount + ", deSerTime: " + DurationFormatUtils.formatDurationHMS(deserTime) + ", request time: " + DurationFormatUtils.formatDurationHMS(reqTime));
                            return false;
                        }

                        //                            while (doc == null && docIt.hasNext())
                        //                            {
                        //                                doc = docIt.next();
                        //                            }
                        //
                        //                            if (doc == null && !docIt.hasNext())
                        //                            {
                        //                                return false;
                        //                            }

                        docCount++;
                        Payload payload = doc.source.payload.get(table);
                        
                        if (tableAliasColumnIndices == null)
                        {
                            tableAliasColumnIndices = new int[tableAlias.getColumns().length];
                            int index = 0;
                            for (String aliasColumn : tableAlias.getColumns())
                            {
                                int columnIndex = ArrayUtils.indexOf(payload.columns, aliasColumn);
                                tableAliasColumnIndices[index++] = columnIndex;
                            }
                        }

                        rowsIterator = payload.rows.iterator();
                        continue;
                        //                            
                        //                            JsonToken token = parser.nextToken();
                        //                            String name = null;
                        //                            if (scrollId.getValue() == null)
                        //                            {
                        //                                while (!"_scroll_id".equals(name) && token != null)
                        //                                {
                        //                                    token = parser.nextToken();
                        //                                    name = parser.getCurrentName();
                        //                                }
                        //                                if (token == null)
                        //                                {
                        //                                    continue;
                        //                                }
                        //                                scrollId.setValue(parser.getText());
                        //                            }
                        //
                        //                            while (!"payload".equals(name) && token != null)
                        //                            {
                        //                                token = parser.nextToken();
                        //                                name = parser.getCurrentName();
                        //                            }
                        //                            if (token == null)
                        //                            {
                        //                                // No payload found, we're done
                        //                                client.close();
                        //                                response.close();
                        //                                parser.close();
                        //                                
                        //                                sw.stop();
                        ////                                System.out.println();
                        ////                                System.out.println("Stats for " + table + " quueryCount: " + queryCount + ", time: " + sw.toString() + ", docCount: " + docCount + ", rowCount: " + rowCount + ", deSerTime: " + DurationFormatUtils.formatDurationHMS(deserTime) + ", request time: " + DurationFormatUtils.formatDurationHMS(reqTime));
                        //                                
                        //                                return false;
                        //                            }
                        //                            docCount++;
                        //                            token = parser.nextToken();
                        //
                        //                            // Find current table
                        //                            while (!table.equals(name) && token != null)
                        //                            {
                        //                                token = parser.nextToken();
                        //                                name = parser.getCurrentName();
                        //                            }
                        //                            if (token == null)
                        //                            {
                        //                                continue;
                        //                            }
                        //                            token = parser.nextToken();
                        //
                        ////                            assertToken(JsonToken.START_OBJECT, token);
                        //
                        //                            // Fetch columns
                        //                            // TODO: fetch both rows/column in one chunk here to avoid problems if columns
                        //                            //       should appear after rows
                        //                            if (columns == null)
                        //                            {
                        //                                while (!"columns".equals(name) && token != null)
                        //                                {
                        //                                    token = parser.nextToken();
                        //                                    name = parser.getCurrentName();
                        //                                }
                        //                                if (token == null)
                        //                                {
                        //                                    continue;
                        //                                }
                        //
                        //                                token = parser.nextToken();
                        //                                assertToken(JsonToken.START_ARRAY, token);
                        //                                columns = parser.readValueAs(String[].class);
                        //
                        //                                int index = 0;
                        //                                for (String aliasColumn : tableAlias.getColumns())
                        //                                {
                        //                                    int columnIndex = ArrayUtils.indexOf(columns, aliasColumn);
                        //                                    tableAliasColumnIndices[index++] = columnIndex;
                        //                                }
                        //                            }
                        //
                        //                            // Fetch rows
                        //                            while (!"rows".equals(name) && token != null)
                        //                            {
                        //                                token = parser.nextToken();
                        //                                name = parser.getCurrentName();
                        //                            }
                        //                            if (token == null)
                        //                            {
                        //                                continue;
                        //                            }
                        //
                        //                            token = parser.nextToken();
                        //                            assertToken(JsonToken.START_ARRAY, token);
                        //
                        //                            sw2.start();
                        //                            List<Object[]> rows = parser.readValueAs(LIST_OF_OBJECT_ARRAY);
                        //                            sw2.stop();
                        //                            deserTime += sw2.getTime(TimeUnit.MILLISECONDS);
                        //                            sw2.reset();
                        //                            
                        //                            rowCount += rows.size();
                        //                            rowsIterator = rows.iterator();
                        //                            continue;
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
                        rowData[i] = columnIndex >= 0 && columnIndex < data.length ? data[columnIndex] : null;
                    }

                    next = Row.of(tableAlias, rowPos++, rowData);
                    //                    }
                    //                    catch (IOException e)
                    //                    {
                    //                        throw new RuntimeException("Error straming", e);
                    //                    }
                }
                return true;
            }
        };
    }

//    private static void assertToken(JsonToken expected, JsonToken actual)
//    {
//        if (expected != actual)
//        {
//            throw new RuntimeException("Expected " + expected + ", got " + actual);
//        }
//    }

    private static class GetResponse
    {
        @JsonProperty("docs")
        List<Doc> docs;
        //
        //      public void set(List<Doc> docs)
        //      {
        //          docs.removeIf(doc -> doc.source == null);
        //          this.docs = docs;
        //      }
    }

    private static class Doc
    {
        @JsonProperty("_source")
        Source source;

        public boolean isValid()
        {
            return source != null && source.payload != null;
        }
    }

    private static class Source
    {
        @JsonProperty
        Map<String, Payload> payload;
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
        return tableAlias.toString();
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
            OutputStream bos = outStream;//new DebugBos(outStream);//new BufferedOutputStream(outStream, 16000);
            //            bos = new DebugBos(bos);
//            PrintStream ps = new PrintStream(bos, true);
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
                
//                ps.format(pattern, values.next());
//                ps.flush();

                bos.write(QUOTE_BYTES);
            }

            bos.write(FOOTER_BYTES);
            bos.flush();
//            ps.close();
            bos.close();
        }

        private static class DebugBos extends OutputStream
        {
            private final OutputStream os;

            DebugBos(OutputStream os)
            {
                this.os = os;
            }

            @Override
            public void write(int b) throws IOException
            {
                System.out.print((char) b);
                os.write(b);
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
