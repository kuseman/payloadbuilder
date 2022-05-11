package se.kuseman.payloadbuilder.catalog.es;

import static se.kuseman.payloadbuilder.catalog.es.HttpClientUtils.execute;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.core.type.TypeReference;

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.operator.Row;
import se.kuseman.payloadbuilder.api.operator.Tuple;

/** TVF that exposes ES cat api */
class CatFunction extends TableFunctionInfo
{
    private static final TypeReference<List<Map<String, Object>>> LIST_OF_OBJ = new TypeReference<List<Map<String, Object>>>()
    {
    };

    CatFunction(Catalog catalog)
    {
        super(catalog, "cat");
    }

    @Override
    public String getDescription()
    {
        return "Function that exposes Elastic search cat api. " + System.lineSeparator()
               + "cat([<endpoint expression>], <cat spec. expression>) "
               + System.lineSeparator()
               + "ex  select * "
               + System.lineSeparator()
               + "    from es#cat('nodes?s=name:desc')"
               + System.lineSeparator()
               + "See: https://www.elastic.co/guide/en/elasticsearch/reference/current/cat.html";
    }

    @Override
    public TupleIterator open(IExecutionContext context, String catalogAlias, TableAlias tableAlias, List<? extends IExpression> arguments)
    {
        String catspec;
        String endpoint;

        if (arguments.size() == 1)
        {
            endpoint = context.getSession()
                    .getCatalogProperty(catalogAlias, ESCatalog.ENDPOINT_KEY);
            catspec = String.valueOf(arguments.get(0)
                    .eval(context));
        }
        else if (arguments.size() == 2)
        {
            endpoint = String.valueOf(arguments.get(0)
                    .eval(context));
            catspec = String.valueOf(arguments.get(1)
                    .eval(context));
        }
        else
        {
            throw new IllegalArgumentException("cat function takes 1 or 2 arguments. 'cat([<endpoint expression>], <cat spec. expression>)'");
        }

        if (endpoint == null
                || "".equals(endpoint))
        {
            throw new IllegalArgumentException("Missing endpoint in arguments or catalog properties.");
        }

        if (catspec == null
                || "".equals(endpoint))
        {
            throw new IllegalArgumentException("Missing cat speficitation.");
        }

        List<Map<String, Object>> result;
        HttpEntity entity = null;
        HttpGet get = new HttpGet(getCatUrl(endpoint, catspec));
        try (CloseableHttpResponse response = execute(context.getSession(), catalogAlias, get))
        {
            entity = response.getEntity();
            if (response.getStatusLine()
                    .getStatusCode() != HttpStatus.SC_OK)
            {
                String body = IOUtils.toString(response.getEntity()
                        .getContent(), StandardCharsets.UTF_8);
                throw new RuntimeException("Error querying ES cat-api: " + body);
            }

            result = ESOperator.MAPPER.readValue(entity.getContent(), LIST_OF_OBJ);
        }
        catch (Exception e)
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Error querying ES cat-api", e);
        }
        finally
        {
            EntityUtils.consumeQuietly(entity);
        }

        String[] columns = result.get(0)
                .keySet()
                .toArray(ArrayUtils.EMPTY_STRING_ARRAY);
        return TupleIterator.wrap(result.stream()
                .map(map -> (Tuple) Row.of(tableAlias, columns, new Row.MapValues(map, columns)))
                .iterator());
    }

    private String getCatUrl(String endpoint, String catspec)
    {
        URIBuilder builder = ESUtils.uriBuilder(endpoint);
        int index = catspec.indexOf("?");
        if (index >= 0)
        {
            // indices?s=doc.count&
            builder.setPath("_cat/" + catspec.substring(0, index));
            String catargs = catspec.substring(index + 1);
            String[] args = catargs.split("&");
            for (String arg : args)
            {
                String[] pair = arg.split("=");
                builder.addParameter(pair[0], pair.length > 1 ? pair[1]
                        : "");
            }
        }
        else
        {
            builder.setPath("_cat/" + catspec);
        }
        builder.addParameter("format", "json");
        return ESUtils.toUrl(builder);
    }
}