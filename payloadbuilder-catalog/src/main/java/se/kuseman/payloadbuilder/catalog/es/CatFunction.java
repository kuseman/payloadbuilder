package se.kuseman.payloadbuilder.catalog.es;

import static java.util.stream.Collectors.toList;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.net.URIBuilder;

import com.fasterxml.jackson.core.type.TypeReference;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** TVF that exposes ES cat api */
class CatFunction extends TableFunctionInfo
{
    private static final TypeReference<List<Map<String, Object>>> LIST_OF_OBJ = new TypeReference<List<Map<String, Object>>>()
    {
    };

    CatFunction()
    {
        super("cat");
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
    public TupleIterator execute(IExecutionContext context, String catalogAlias, Optional<Schema> schema, List<IExpression> arguments, List<Option> options)
    {
        String catspec;
        String endpoint;

        if (arguments.size() == 1)
        {
            endpoint = context.getSession()
                    .getCatalogProperty(catalogAlias, ESCatalog.ENDPOINT_KEY)
                    .valueAsString(0);
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

        HttpGet get = new HttpGet(getCatUrl(endpoint, catspec));
        List<Map<String, Object>> result;

        try
        {
            result = HttpClientUtils.execute(context.getSession(), catalogAlias, get, response ->
            {
                HttpEntity entity = response.getEntity();
                if (response.getCode() != HttpStatus.SC_OK)
                {
                    String body = IOUtils.toString(response.getEntity()
                            .getContent(), StandardCharsets.UTF_8);
                    throw new RuntimeException("Error querying ES cat-api: " + body);
                }

                return ESDatasource.MAPPER.readValue(entity.getContent(), LIST_OF_OBJ);
            });
        }
        catch (Exception e)
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Error querying ES cat-api", e);
        }

        if (result.isEmpty())
        {
            return TupleIterator.EMPTY;
        }

        Schema catSchema = new Schema(result.get(0)
                .keySet()
                .stream()
                .map(c -> Column.of(c, ResolvedType.of(Type.Any)))
                .collect(toList()));

        return TupleIterator.singleton(new ObjectTupleVector(catSchema, result.size(), (row, col) ->
        {
            Map<String, Object> map = result.get(row);
            Column column = catSchema.getColumns()
                    .get(col);
            return map.get(column.getName());
        }));
    }

    static String getCatUrl(String endpoint, String catspec)
    {
        URIBuilder builder = ESQueryUtils.uriBuilder(endpoint);
        String existingPath = Objects.toString(builder.getPath(), "");
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
            builder.setPath(existingPath + "_cat/"
                            + (catspec.charAt(0) == '/' ? catspec.substring(1, catspec.length())
                                    : catspec));
        }
        builder.addParameter("format", "json");
        return ESQueryUtils.toUrl(builder);
    }
}
