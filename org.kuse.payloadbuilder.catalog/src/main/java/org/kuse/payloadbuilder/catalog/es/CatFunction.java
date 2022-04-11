package org.kuse.payloadbuilder.catalog.es;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Operator.TupleIterator;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.Expression;

import com.fasterxml.jackson.core.type.TypeReference;

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
            + "cat([<endpoint expression>], <cat spec. expression>) " + System.lineSeparator()
            + "ex  select * " + System.lineSeparator()
            + "    from es#cat('nodes?s=name:desc')" + System.lineSeparator()
            + "See: https://www.elastic.co/guide/en/elasticsearch/reference/current/cat.html";
    }

    @Override
    public TupleIterator open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments)
    {
        String catspec;
        String endpoint;

        if (arguments.size() == 1)
        {
            endpoint = context.getSession().getCatalogProperty(catalogAlias, ESCatalog.ENDPOINT_KEY);
            catspec = String.valueOf(arguments.get(0).eval(context));
        }
        else if (arguments.size() == 2)
        {
            endpoint = String.valueOf(arguments.get(0).eval(context));
            catspec = String.valueOf(arguments.get(1).eval(context));
        }
        else
        {
            throw new IllegalArgumentException("cat function takes 1 or 2 arguments. 'cat([<endpoint expression>], <cat spec. expression>)'");
        }

        if (isBlank(endpoint))
        {
            throw new IllegalArgumentException("Missing endpoint in arguments or catalog properties.");
        }

        if (isBlank(catspec))
        {
            throw new IllegalArgumentException("Missing cat speficitation.");
        }

        List<Map<String, Object>> result;
        HttpEntity entity = null;
        HttpGet get = new HttpGet(getCatUrl(endpoint, catspec));
        get.addHeader(HttpHeaders.ACCEPT_ENCODING, "gzip");
        try (CloseableHttpResponse response = ESOperator.CLIENT.execute(get))
        {
            entity = response.getEntity();
            if (response.getStatusLine().getStatusCode() != HttpStatus.SC_OK)
            {
                String body = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
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

        String[] columns = result.get(0).keySet().toArray(ArrayUtils.EMPTY_STRING_ARRAY);
        return TupleIterator.wrap(result.stream().map(map -> (Tuple) Row.of(tableAlias, columns, new Row.MapValues(map, columns))).iterator());
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
                builder.addParameter(pair[0], pair.length > 1 ? pair[1] : "");
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
