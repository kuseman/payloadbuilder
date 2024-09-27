package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Collections.emptyMap;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.io.entity.StringEntity;

import com.fasterxml.jackson.core.JsonProcessingException;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Render a template in ES */
class RenderTemplateFunction extends ScalarFunctionInfo
{
    RenderTemplateFunction()
    {
        super("rendertemplate", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Function that renders a script template using /render/_template endpoint. " + System.lineSeparator()
               + "ex. rendertemplate(<template expression>, <params expression>) "
               + System.lineSeparator();
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public Arity arity()
    {
        return Arity.TWO;
    }

    @SuppressWarnings("unchecked")
    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        ValueVector vv = arguments.get(0)
                .eval(context);
        if (vv.isNull(0))
        {
            return ValueVector.literalNull(ResolvedType.STRING, 1);
        }

        // CSOFF
        String template = vv.getString(0)
                .toString();
        // CSON

        vv = arguments.get(1)
                .eval(context);

        Map<String, Object> params = emptyMap();

        if (!vv.isNull(0))
        {
            Object paramsObj = vv.getAny(0);
            if (!(paramsObj instanceof Map))
            {
                throw new IllegalArgumentException("Expected params argument for " + getName() + " to return a Map, got: " + paramsObj);
            }
            params = (Map<String, Object>) paramsObj;
        }

        String endpoint = context.getSession()
                .getCatalogProperty(catalogAlias, ESCatalog.ENDPOINT_KEY)
                .valueAsString(0);

        if (isBlank(endpoint))
        {
            throw new IllegalArgumentException("Missing endpoint in catalog properties.");
        }

        HttpPost post = new HttpPost(endpoint + "/_render/template/");
        post.setEntity(new StringEntity(serialize(ofEntries(entry("id", template), entry("params", params))), StandardCharsets.UTF_8));

        try
        {
            return HttpClientUtils.execute(context.getSession(), catalogAlias, post, response ->
            {
                HttpEntity entity = response.getEntity();
                return ValueVector.literalString(IOUtils.toString(entity.getContent(), StandardCharsets.UTF_8), 1);
            });
        }
        catch (Exception e)
        {
            if (e instanceof RuntimeException)
            {
                throw (RuntimeException) e;
            }
            throw new RuntimeException("Error rendering template", e);
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
