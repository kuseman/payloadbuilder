package se.kuseman.payloadbuilder.catalog.es;

import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static se.kuseman.payloadbuilder.catalog.es.ESQueryUtils.getScrollUrl;
import static se.kuseman.payloadbuilder.catalog.es.ESQueryUtils.getSearchUrl;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.INamedExpression;

/** Search ES */
class SearchFunction extends TableFunctionInfo
{
    SearchFunction()
    {
        super("search");
    }

    @Override
    public String getDescription()
    {
        return "Table function that enables a raw elasticsearch query to be made. " + System.lineSeparator()
               + "Function requires named parameters: "
               + System.lineSeparator()
               + " - endpoint (Optional String): Endpoint to elasticsearch. (*)"
               + System.lineSeparator()
               + " - index (Optional String): Index in elasticsearch. (*)"
               + System.lineSeparator()
               + " - type (Optional String): Type in elasticsearch. Defaults to _doc"
               + System.lineSeparator()
               + " - body (String): Search body to use. Mutual exclusive with 'template'"
               + System.lineSeparator()
               + " - template (String): Search template to use. Mutual exclusive with 'body'."
               + System.lineSeparator()
               + " - params (Optional String): A map expression with template parameters. Only applicable when using 'template'"
               + System.lineSeparator()
               + " (*) - If not provided then value is resolved from catalog properties."
               + System.lineSeparator()
               + System.lineSeparator()
               + "Example with body: "
               + System.lineSeparator()
               + "    select * "
               + System.lineSeparator()
               + "    from es#search("
               + System.lineSeparator()
               + "      body: '{ \"query\": { \"term\": { \"field\": 123 } } }'"
               + System.lineSeparator()
               + "    )"
               + System.lineSeparator()
               + System.lineSeparator()
               + "Example with template: "
               + System.lineSeparator()
               + "    select * "
               + System.lineSeparator()
               + "    from es#search("
               + System.lineSeparator()
               + "      template: 'searchTemplate',"
               + System.lineSeparator()
               + "      params: '{ \"key\": 123, \"key2\": \"value\" }'"
               + System.lineSeparator()
               + "    )"
               + System.lineSeparator()
               + System.lineSeparator();
    }

    @Override
    public boolean requiresNamedArguments()
    {
        return true;
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, Optional<Schema> schema, List<IExpression> arguments, List<Option> options)
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
            if ("endpoint".equals(name))
            {
                endpoint = getArg(context, namedArg, String.class, "endpoint");
            }
            else if ("index".equals(name))
            {
                index = getArg(context, namedArg, String.class, "index");
            }
            else if ("type".equals(name))
            {
                type = getArg(context, namedArg, String.class, "type");
            }
            else if ("body".equals(name))
            {
                body = getArg(context, namedArg, String.class, "body");
            }
            else if ("template".equals(name))
            {
                template = getArg(context, namedArg, String.class, "template");
            }
            else if ("scroll".equals(name))
            {
                scroll = getArg(context, namedArg, Boolean.class, "scroll");
            }
            else if ("params".equals(name))
            {
                Object obj = namedArg.eval(context);
                if (obj instanceof String)
                {
                    params = (String) obj;
                }
                else if (obj instanceof Map)
                {
                    try
                    {
                        params = ESDatasource.MAPPER.writeValueAsString(obj);
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

        int batchSize = scroll ? context.getBatchSize(options)
                : -1;

        // Turn off the options batch size if we have a size in the body
        // else the body value will get overridden
        if (body != null
                && body.contains("\"size\":"))
        {
            batchSize = -1;
        }

        ElasticsearchMeta meta = ElasticsearchMetaUtils.getMeta(context.getSession(), catalogAlias, endpoint, index);
        String searchUrl = getSearchUrl(endpoint, index, type, batchSize < 0 ? null
                : batchSize,
                scroll ? 2
                        : null,
                !isBlank(template));
        String scrollUrl = scroll ? getScrollUrl(endpoint, 2)
                : null;

        String actualBody = getBody(body, template, defaultIfBlank(params, "{}"));
        return ESDatasource.getScrollingIterator(context, meta.getStrategy(), catalogAlias, endpoint, new ESDatasource.Data(), searchUrl, scrollUrl, actualBody);
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
        ValueVector v = expression.eval(context);
        if (v.isNull(0))
        {
            return null;
        }

        Object obj = v.valueAsObject(0);
        if (obj instanceof UTF8String)
        {
            obj = ((UTF8String) obj).toString();
        }
        if (!clazz.isAssignableFrom(obj.getClass()))
        {
            throw new IllegalArgumentException("Expected a " + clazz.getSimpleName() + " for argument " + key + " for function " + getName() + " but got: " + obj);
        }
        return (T) obj;
    }
}
