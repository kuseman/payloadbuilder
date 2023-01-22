package se.kuseman.payloadbuilder.catalog.es;

import java.net.URI;
import java.nio.charset.StandardCharsets;

import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

/** Strategy with basic common functionality cross versions */
class GenericStrategy implements ElasticStrategy
{
    @Override
    public HttpRequestBase getScrollRequest(String scrollUrl, String scrollId)
    {
        HttpPost post = new HttpPost(scrollUrl);
        post.setEntity(new StringEntity("{\"scroll_id\":\"" + scrollId + "\" }", StandardCharsets.UTF_8));
        return post;
    }

    @Override
    public HttpRequestBase getDeleteScrollRequest(String endpoint, String scrollId)
    {
        HttpDeleteWithBody delete = new HttpDeleteWithBody(endpoint + "/_search/scroll");
        delete.setEntity(new StringEntity("{\"scroll_id\":\"" + scrollId + "\"}", StandardCharsets.UTF_8));
        return delete;
    }

    @Override
    public boolean supportsFilterInBoolQuery()
    {
        return true;
    }

    @Override
    public boolean supportsTypes()
    {
        return true;
    }

    @Override
    public boolean wrapNestedSortPathInObject()
    {
        return true;
    }

    /** Entity support for DELETE */
    static class HttpDeleteWithBody extends HttpEntityEnclosingRequestBase
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

}
