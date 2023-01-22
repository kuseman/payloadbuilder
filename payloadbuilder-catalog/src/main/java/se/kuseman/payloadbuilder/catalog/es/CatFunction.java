package se.kuseman.payloadbuilder.catalog.es;

import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.defaultString;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.core.type.TypeReference;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

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
    public TupleIterator execute(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments, IDatasourceOptions options)
    {
        String catspec;
        String endpoint;

        if (arguments.size() == 1)
        {
            endpoint = context.getSession()
                    .getCatalogProperty(catalogAlias, ESCatalog.ENDPOINT_KEY);
            catspec = String.valueOf(arguments.get(0)
                    .eval(context)
                    .valueAsObject(0));
        }
        else if (arguments.size() == 2)
        {
            endpoint = String.valueOf(arguments.get(0)
                    .eval(context)
                    .valueAsObject(0));
            catspec = String.valueOf(arguments.get(1)
                    .eval(context)
                    .valueAsObject(0));
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
        try (CloseableHttpResponse response = HttpClientUtils.execute(context.getSession(), catalogAlias, get))
        {
            entity = response.getEntity();
            if (response.getStatusLine()
                    .getStatusCode() != HttpStatus.SC_OK)
            {
                String body = IOUtils.toString(response.getEntity()
                        .getContent(), StandardCharsets.UTF_8);
                throw new RuntimeException("Error querying ES cat-api: " + body);
            }

            result = ESDatasource.MAPPER.readValue(entity.getContent(), LIST_OF_OBJ);
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

        if (result.isEmpty())
        {
            return TupleIterator.EMPTY;
        }

        Schema schema = new Schema(result.get(0)
                .keySet()
                .stream()
                .map(c -> Column.of(c, ResolvedType.of(Type.Any)))
                .collect(toList()));

        return TupleIterator.singleton(new ObjectTupleVector(schema, result.size(), (row, col) ->
        {
            Map<String, Object> map = result.get(row);
            Column column = schema.getColumns()
                    .get(col);
            return map.get(column.getName());
        }));
        // String[] columns = result.get(0)
        // .keySet()
        // .toArray(ArrayUtils.EMPTY_STRING_ARRAY);
        // return TupleIterator.wrap(result.stream()
        // .map(map -> (Tuple) Row.of(tableAlias, columns, new Row.MapValues(map, columns)))
        // .iterator());
    }

    static String getCatUrl(String endpoint, String catspec)
    {
        URIBuilder builder = ESQueryUtils.uriBuilder(endpoint);
        String existingPath = defaultString(builder.getPath(), "");
        if (!existingPath.endsWith("/"))
        {
            existingPath = existingPath + "/";
        }
        int index = catspec.indexOf("?");
        if (index >= 0)
        {
            // indices?s=doc.count&
            builder.setPath(existingPath + "_cat/" + catspec.substring(0, index));
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
            builder.setPath(existingPath + "_cat/" + catspec);
        }
        builder.addParameter("format", "json");
        return ESQueryUtils.toUrl(builder);
    }
}
