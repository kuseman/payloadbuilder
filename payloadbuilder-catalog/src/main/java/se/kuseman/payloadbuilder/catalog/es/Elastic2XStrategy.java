package se.kuseman.payloadbuilder.catalog.es;

import java.nio.charset.StandardCharsets;

import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

/** Strategy used for elastic 2.x versions */
class Elastic2XStrategy extends GenericStrategy
{
    @Override
    public HttpRequestBase getDeleteScrollRequest(String endpoint, String scrollId)
    {
        HttpDeleteWithBody delete = new HttpDeleteWithBody(endpoint + "/_search/scroll");
        delete.setEntity(new StringEntity(scrollId, StandardCharsets.UTF_8));
        return delete;
    }

    @Override
    public boolean wrapNestedSortPathInObject()
    {
        return false;
    }
}
