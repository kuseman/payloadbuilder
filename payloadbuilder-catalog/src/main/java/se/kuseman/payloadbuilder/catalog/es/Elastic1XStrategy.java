package se.kuseman.payloadbuilder.catalog.es;

import java.nio.charset.StandardCharsets;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

/** Strategy used for elastic 1.x versions */
class Elastic1XStrategy extends GenericStrategy
{
    @Override
    public HttpRequestBase getScrollRequest(String scrollUrl, String scrollId)
    {
        HttpPost post = new HttpPost(scrollUrl);
        post.removeHeaders(HttpHeaders.CONTENT_TYPE);
        post.setEntity(new StringEntity(scrollId, StandardCharsets.UTF_8));
        return post;
    }

    @Override
    public HttpRequestBase getDeleteScrollRequest(String endpoint, String scrollId)
    {
        HttpDeleteWithBody delete = new HttpDeleteWithBody(endpoint + "/_search/scroll");
        delete.setEntity(new StringEntity(scrollId, StandardCharsets.UTF_8));
        return delete;
    }

    @Override
    public boolean supportsFilterInBoolQuery()
    {
        return false;
    }

    @Override
    public boolean wrapNestedSortPathInObject()
    {
        return false;
    }
}
