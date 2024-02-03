package se.kuseman.payloadbuilder.catalog.es;

import java.nio.charset.StandardCharsets;

import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.http.io.entity.StringEntity;

/** Strategy with basic common functionality cross versions */
class GenericStrategy implements ElasticStrategy
{
    @Override
    public HttpUriRequestBase getScrollRequest(String scrollUrl, String scrollId)
    {
        HttpPost post = new HttpPost(scrollUrl);
        post.setEntity(new StringEntity("{\"scroll_id\":\"" + scrollId + "\" }", StandardCharsets.UTF_8));
        return post;
    }

    @Override
    public HttpUriRequestBase getDeleteScrollRequest(String endpoint, String scrollId)
    {
        HttpDelete delete = new HttpDelete(endpoint + "/_search/scroll");
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
}
