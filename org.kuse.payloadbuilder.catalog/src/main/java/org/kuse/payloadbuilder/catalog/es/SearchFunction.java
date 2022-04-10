package org.kuse.payloadbuilder.catalog.es;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Operator.TupleIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.NamedExpression;
import org.kuse.payloadbuilder.core.utils.ObjectUtils;

/** Search ES */
class SearchFunction extends TableFunctionInfo
{
    private static final int SCROLL_SIZE = 1000;

    SearchFunction(Catalog catalog)
    {
        super(catalog, "search");
    }

    @Override
    public boolean requiresNamedArguments()
    {
        return true;
    }

    //CSOFF
    @Override
    //CSON
    public TupleIterator open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments)
    {
        String endpoint = null;
        String index = null;
        String type = null;
        String body = null;
        String params = null;
        String template = null;
        boolean scroll = false;

        for (Expression arg : arguments)
        {
            NamedExpression namedArg = (NamedExpression) arg;
            String name = namedArg.getName();
            Expression expression = namedArg.getExpression();
            if ("endpoint".equals(name))
            {
                endpoint = getArg(context, expression, String.class, "endpoint");
            }
            else if ("index".equals(name))
            {
                index = getArg(context, expression, String.class, "index");
            }
            else if ("type".equals(name))
            {
                type = getArg(context, expression, String.class, "type");
            }
            else if ("body".equals(name))
            {
                body = getArg(context, expression, String.class, "body");
            }
            else if ("template".equals(name))
            {
                template = getArg(context, expression, String.class, "template");
            }
            else if ("scroll".equals(name))
            {
                scroll = getArg(context, expression, Boolean.class, "scroll");
            }
            else if ("params".equals(name))
            {
                Object obj = expression.eval(context);
                if (obj instanceof String)
                {
                    params = (String) obj;
                }
                else if (obj instanceof Map)
                {
                    try
                    {
                        params = ESOperator.MAPPER.writeValueAsString(obj);
                    }
                    catch (IOException e)
                    {
                        throw new RuntimeException("Error deserializing: " + obj, e);
                    }
                }
                else if (params != null)
                {
                    throw new IllegalArgumentException("Expected 'params' as String or Map but bot: " + obj);
                }
            }
        }

        if (endpoint == null)
        {
            endpoint = ESType.getEndpoint(context.getSession(), catalogAlias);
        }
        if (index == null)
        {
            index = ESType.getIndex(context.getSession(), catalogAlias);
        }

        if (template != null && body != null)
        {
            throw new IllegalArgumentException("'template' and 'body' arguments are mutual exclusive for function " + getName());
        }

        final String searchUrl = !isBlank(template)
            ? getSearchTemplateUrl(
                    endpoint,
                    index,
                    type,
                    scroll ? SCROLL_SIZE : null,
                    scroll ? 2 : null,
                    tableAlias,
                    null)
            : ESOperator.getSearchUrl(
                    endpoint,
                    index,
                    type,
                    scroll ? SCROLL_SIZE : null,
                    scroll ? 2 : null,
                    tableAlias,
                    null);
        final String scrollUrl = scroll ? ESOperator.getScrollUrl(endpoint, type, 2, tableAlias, null) : null;

        AtomicLong sentBytes = new AtomicLong();
        String actualBody = getBody(body, template, defaultIfBlank(params, "{}"));
        MutableBoolean doRequest = new MutableBoolean(true);
        return ESOperator.getIterator(
                context,
                tableAlias,
                endpoint,
                ESCatalog.SINGLE_TYPE_TABLE_NAME.equals(type),
                new ESOperator.Data(),
                scrollId ->
                {
                    if (doRequest.getValue())
                    {
                        sentBytes.addAndGet(searchUrl.length() + actualBody.length());
                        HttpPost post = new HttpPost(searchUrl);
                        post.setEntity(new StringEntity(actualBody, UTF_8));
                        doRequest.setFalse();
                        return post;
                    }
                    else if (scrollUrl != null)
                    {
                        String id = scrollId.getValue();
                        scrollId.setValue(null);
                        HttpPost post = new HttpPost(scrollUrl);
                        post.setEntity(new StringEntity(id, UTF_8));
                        return post;
                    }

                    return null;
                });
    }

    private static String getSearchTemplateUrl(
            String endpoint,
            String index,
            String type,
            Integer size,
            Integer scrollMinutes,
            TableAlias alias,
            String indexField)
    {
        boolean isSingleType = ESCatalog.SINGLE_TYPE_TABLE_NAME.equals(type);
        ObjectUtils.requireNonBlank(endpoint, "endpoint is required");
        return ESOperator.getUrl(String.format("%s/%s/%s_search/template?%s",
                endpoint,
                isBlank(index) ? "*" : index,
                isBlank(type) ? "" : (type + "/"),
                scrollMinutes != null ? ("scroll=" + scrollMinutes + "m") : "",
                size != null ? ("&size=" + size) : ""), "hits.hits", alias, indexField, isSingleType);
    }

    private String getBody(String body, String template, String params)
    {
        if (!isBlank(body))
        {
            return body;
        }
        else if (!isBlank(template))
        {
            return "{ \"id\":\"" + template + "\", \"params\":" + params + "}";
        }
        return "";
    }

    @SuppressWarnings("unchecked")
    private <T> T getArg(ExecutionContext context, Expression argument, Class<? extends T> clazz, String key)
    {
        Object obj = argument.eval(context);
        if (obj == null)
        {
            return null;
        }
        if (!clazz.isAssignableFrom(obj.getClass()))
        {
            throw new IllegalArgumentException("Expected a " + clazz.getSimpleName() + " for argument " + key + " for function " + getName() + " but got: " + obj);
        }
        return (T) obj;
    }
}
