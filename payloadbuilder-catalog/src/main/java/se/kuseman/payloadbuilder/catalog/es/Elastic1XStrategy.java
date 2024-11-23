package se.kuseman.payloadbuilder.catalog.es;

import java.nio.charset.StandardCharsets;

import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.io.entity.StringEntity;

/** Strategy used for elastic 1.x versions */
class Elastic1XStrategy extends GenericStrategy
{
    @Override
    public HttpUriRequestBase getScrollRequest(String scrollUrl, String scrollId)
    {
        HttpPost post = new HttpPost(scrollUrl);
        post.removeHeaders(HttpHeaders.CONTENT_TYPE);
        post.setEntity(new StringEntity(scrollId, StandardCharsets.UTF_8));
        return post;
    }

    @Override
    public HttpUriRequestBase getDeleteScrollRequest(String endpoint, String scrollId)
    {
        HttpDelete delete = new HttpDelete(endpoint + "/_search/scroll");
        delete.setEntity(new StringEntity(scrollId, StandardCharsets.UTF_8));
        return delete;
    }

    @Override
    public boolean supportsFilterInBoolQuery()
    {
        return false;
    }

    @Override
    public boolean supportsMatchNone()
    {
        return false;
    }

    @Override
    public boolean wrapNestedSortPathInObject()
    {
        return false;
    }

    @Override
    public boolean supportsDataStreams()
    {
        return false;
    }
}
