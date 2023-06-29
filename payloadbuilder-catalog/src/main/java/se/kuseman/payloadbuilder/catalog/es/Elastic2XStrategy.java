package se.kuseman.payloadbuilder.catalog.es;

import java.nio.charset.StandardCharsets;

import org.apache.hc.client5.http.classic.methods.HttpDelete;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.io.entity.StringEntity;

/** Strategy used for elastic 2.x versions */
class Elastic2XStrategy extends GenericStrategy
{
    @Override
    public ClassicHttpRequest getDeleteScrollRequest(String endpoint, String scrollId)
    {
        HttpDelete delete = new HttpDelete(endpoint + "/_search/scroll");
        delete.setEntity(new StringEntity(scrollId, StandardCharsets.UTF_8));
        return delete;
    }

    @Override
    public boolean wrapNestedSortPathInObject()
    {
        return false;
    }
}
