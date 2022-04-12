package se.kuseman.payloadbuilder.catalog.es;

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

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.INamedExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.utils.StringUtils;

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

    @Override
    public TupleIterator open(IExecutionContext context, String catalogAlias, TableAlias tableAlias, List<? extends IExpression> arguments)
    {
        String endpoint = null;
        String index = null;
        String type = null;
        String body = null;
        String params = null;
        String template = null;
        boolean scroll = false;

        int size = arguments.size();

        for (int i = 0; i < size; i++)
        {
            INamedExpression namedArg = (INamedExpression) arguments.get(i);
            String name = namedArg.getName();
            IExpression expression = namedArg.getExpression();
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

        if (type == null)
        {
            throw new IllegalArgumentException("'type' argument must be specified for function " + getName());
        }

        if (template == null
                && body == null)
        {
            throw new IllegalArgumentException("One of 'template' or 'body' arguments must be specified for function " + getName());
        }

        if (template != null
                && body != null)
        {
            throw new IllegalArgumentException("'template' and 'body' arguments are mutual exclusive for function " + getName());
        }

        final String searchUrl = !isBlank(template) ? getSearchTemplateUrl(endpoint, index, type, scroll ? SCROLL_SIZE
                : null,
                scroll ? 2
                        : null)
                : ESOperator.getSearchUrl(endpoint, index, type, scroll ? SCROLL_SIZE
                        : null,
                        scroll ? 2
                                : null,
                        null);
        final String scrollUrl = scroll ? ESOperator.getScrollUrl(endpoint, type, 2, null)
                : null;

        AtomicLong sentBytes = new AtomicLong();
        String actualBody = getBody(body, template, defaultIfBlank(params, "{}"));
        MutableBoolean doRequest = new MutableBoolean(true);
        return ESOperator.getIterator(context, catalogAlias, tableAlias, endpoint, ESCatalog.SINGLE_TYPE_TABLE_NAME.equals(type), new ESOperator.Data(), scrollId ->
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

    private static String getSearchTemplateUrl(String endpoint, String index, String type, Integer size, Integer scrollMinutes)
    {
        // boolean isSingleType = ESCatalog.SINGLE_TYPE_TABLE_NAME.equals(type);
        StringUtils.requireNonBlank(endpoint, "endpoint is required");
        return String.format("%s/%s/%s_search/template?%s&filter_path=_scroll_id,hits.hits", endpoint, isBlank(index) ? "*"
                : index,
                isBlank(type) ? ""
                        : (type + "/"),
                scrollMinutes != null ? ("scroll=" + scrollMinutes + "m")
                        : "",
                size != null ? ("&size=" + size)
                        : "");
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
    private <T> T getArg(IExecutionContext context, IExpression expression, Class<? extends T> clazz, String key)
    {
        Object obj = expression.eval(context);
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
