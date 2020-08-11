package org.kuse.payloadbuilder.catalog.es;

import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.nio.charset.Charset;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Search ES */
class SearchFunction extends TableFunctionInfo
{
    private static final Charset UTF_8 = Charset.forName("UTF-8");
    SearchFunction(Catalog catalog)
    {
        super(catalog, "search");
    }

    @Override
    public Iterator<Row> open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments)
    {
        /*
           - Endpoint set in properties
               search(index, body)
           - No endpoint set in properties
               search(endpoint, index, body)

         */

//        int size = arguments.size();

        String endpoint = getString(context, arguments, "endpoint", 0, (String) context.getSession().getCatalogProperty(catalogAlias, ESCatalog.ENDPOINT_KEY));
        String index = getString(context, arguments, "index", 1, defaultIfNull((String) context.getSession().getCatalogProperty(catalogAlias, ESCatalog.INDEX_KEY), ""));
        String type = getString(context, arguments, "type", 2, "");
        String body = getString(context, arguments, "body", 3, "");

        // No properties
        // size = 4 => endpoint/index/type/body
        // size = 3 => endpoint/index/type
        // size = 2 => endpoint/index
        // size = 1 => endpoint
        
        // Endpoint set
        // size = 3 => index/type/body
        // size = 2 => index/type
        // size = 1 => index
        
        // Index set
        // size = 3 => endpoint/type/body
        // size = 2 => endpoint/type
        // size = 1 => endpoint
        
        // Endpoint + index set
        // size = 2 => type/body
        // size = 1 => type
        
//        int minArgCount = isBlank(endpoint) ? 1 : 0;
//
//        if (size < minArgCount || size > minArgCount + 2)
//        {
//            throw new IllegalArgumentException("Expected " + minArgCount + " or " + (minArgCount + 2) + " arguments for function " + getName() + " but found " + arguments.size());
//        }
//
//        if (isBlank(endpoint))
//        {
//            endpoint = getString(context, arguments, "endpoint", 0);
//            if (isBlank(index) && size > 1)
//            {
//                index = getString(context, arguments, "endpoint", 1);
//            }
//        }
//        else if (isBlank(index))
//        {
//            index = getString(context, arguments, "index", 0);
//        }
//
//        if (size == minArgCount + 2)
//        {
//            type = getString(context, arguments, "type", size - 2);
//            body = getString(context, arguments, "body", size - 1);
//        }
//        else if (size == minArgCount + 1)
//        {
//            type = getString(context, arguments, "type", minArgCount);
//        }

        // TODO: move url:s to common location
        final String searchUrl = String.format(
                "%s%s%s/_search?size=%d&scroll=%s&filter_path=_scroll_id,hits.hits._index,hits.hits._type,hits.hits._id,hits.hits._source",
                endpoint,
                !isBlank(index) ? "/" + index : "",
                !isBlank(type)  
                ? ((isBlank(index) ? "/*" : "") + "/" + type) 
                : "",
                // TODO: table option for batch size
                100,
                "2m");
        final String scrollUrl = String.format("%s/_search/scroll?filter_path=_scroll_id,hits.hits._index,hits.hits._type,hits.hits._idhits.hits._source&scroll=%s", endpoint, "2m");
        AtomicInteger scrollCount = new AtomicInteger();
        AtomicLong sentBytes = new AtomicLong();
        String actualBody = body != null ? body : "";
        return ESOperator.getIterator(tableAlias, new AtomicLong(),
                scrollId ->
                {
                    scrollCount.incrementAndGet();
                    if (scrollId.getValue() == null)
                    {
                        sentBytes.addAndGet(searchUrl.length() + actualBody.length());
                        HttpPost post = new HttpPost(searchUrl);
                        post.setEntity(new StringEntity(actualBody, UTF_8));
                        return post;
                    }
                    else
                    {
                        String id = scrollId.getValue();
                        scrollId.setValue(null);
                        HttpPost post = new HttpPost(scrollUrl);
                        post.setEntity(new StringEntity(id, UTF_8));
                        return post;
                    }
                });
    }

    private String getString(ExecutionContext context, List<Expression> arguments, String key, int index, String defaultValue)
    {
        Object string = defaultIfNull(
                index <arguments.size() ? arguments.get(index).eval(context) : null,
                defaultValue);
        if (!(string instanceof String))
        {
            throw new IllegalArgumentException("Expected a String for " + key + " argument for function " + getName() + " but got: " + string);
        }
        return (String) string;
    }
}
