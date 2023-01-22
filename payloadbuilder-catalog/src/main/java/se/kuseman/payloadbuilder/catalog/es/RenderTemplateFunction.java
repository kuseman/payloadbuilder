package se.kuseman.payloadbuilder.catalog.es;

import static org.apache.commons.lang3.StringUtils.isBlank;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;

import com.fasterxml.jackson.core.JsonProcessingException;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Render a template in ES */
class RenderTemplateFunction extends ScalarFunctionInfo
{
    RenderTemplateFunction(Catalog catalog)
    {
        super(catalog, "rendertemplate", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Function that renders a script template using /render/_template endpoint. " + System.lineSeparator()
               + "ex. rendertemplate(<template expression>, <params expression>) "
               + System.lineSeparator();
    }

    @Override
    public int arity()
    {
        return 2;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object templateObj = arguments.get(0)
                .eval(context);
        if (!(templateObj instanceof String))
        {
            throw new IllegalArgumentException("Expected template argument for " + getName() + " to return a String, got: " + templateObj);
        }
        Object paramsObj = arguments.get(1)
                .eval(context);
        if (!(paramsObj instanceof Map))
        {
            throw new IllegalArgumentException("Expected params argument for " + getName() + " to return a Map, got: " + paramsObj);
        }

        String endpoint = context.getSession()
                .getCatalogProperty(catalogAlias, ESCatalog.ENDPOINT_KEY);

        if (isBlank(endpoint))
        {
            throw new IllegalArgumentException("Missing endpoint in catalog properties.");
        }

        String template = (String) templateObj;
        @SuppressWarnings("unchecked")
        Map<String, Object> params = (Map<String, Object>) paramsObj;

        HttpPost post = new HttpPost(endpoint + "/_render/template/");
        post.setEntity(new StringEntity(serialize(ofEntries(entry("id", template), entry("params", params))), StandardCharsets.UTF_8));
        HttpEntity entity = null;
        try (CloseableHttpResponse response = HttpClientUtils.execute(context.getSession(), catalogAlias, post))
        {
            entity = response.getEntity();
            return ValueVector.literalObject(ResolvedType.of(Type.String), IOUtils.toString(entity.getContent(), StandardCharsets.UTF_8), 1);
        }
        catch (Exception e)
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Error rendering template", e);
        }
        finally
        {
            EntityUtils.consumeQuietly(entity);
        }
    }

    private String serialize(Object value)
    {
        try
        {
            return ESDatasource.MAPPER.writeValueAsString(value);
        }
        catch (JsonProcessingException e)
        {
            throw new RuntimeException("Error serializing params", e);
        }
    }
}
