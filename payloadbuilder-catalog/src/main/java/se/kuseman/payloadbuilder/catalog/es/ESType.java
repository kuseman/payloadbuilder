package se.kuseman.payloadbuilder.catalog.es;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;

/** Class representing a endpoint/index/type combo. */
class ESType
{
    final String endpoint;
    final String index;
    final String type;

    ESType(String endpoint, String index, String type)
    {
        this.endpoint = StringUtils.stripEnd(endpoint, "/");
        this.index = index;
        this.type = type;
    }

    static String getEndpoint(IQuerySession session, String catalogAlias)
    {
        String endpoint = session.getCatalogProperty(catalogAlias, ESCatalog.ENDPOINT_KEY);
        if (isBlank(endpoint))
        {
            throw new IllegalArgumentException("Missing endpoint key in catalog properties for: " + catalogAlias);
        }
        return endpoint;
    }

    static String getIndex(IQuerySession session, String catalogAlias)
    {
        String index = session.getCatalogProperty(catalogAlias, ESCatalog.INDEX_KEY);
        if (isBlank(index))
        {
            throw new IllegalArgumentException("Missing index key in catalog properties for: " + catalogAlias);
        }
        return index;
    }

    /** Create type from provided session/table */
    static ESType of(IQuerySession session, String catalogAlias, QualifiedName table)
    {
        String endpoint;
        String indexName;
        String type;

        List<String> parts = table.getParts();

        // Three part qualified name -> <endpoint>.<index>.<type>
        if (parts.size() == 3)
        {
            endpoint = parts.get(0);
            // If first part is blank then try to get from properties
            // This to support reusing selected endpoint but use another index
            if (isBlank(endpoint))
            {
                endpoint = getEndpoint(session, catalogAlias);
            }
            indexName = parts.get(1);
            type = parts.get(2);
        }
        // Tow or one part qualified name -> <index>.<type> or <type>
        else if (parts.size() <= 2)
        {
            endpoint = getEndpoint(session, catalogAlias);
            if (parts.size() == 2)
            {
                indexName = parts.get(0);
                type = parts.get(1);
            }
            else
            {
                indexName = getIndex(session, catalogAlias);
                type = parts.get(0);
            }
        }
        else
        {
            throw new IllegalArgumentException("Invalid qualified table name " + table + ". Requires 1 to 3 parts. <endpoint>.<index>.<type>");
        }

        return new ESType(endpoint, indexName, type);
    }
}
