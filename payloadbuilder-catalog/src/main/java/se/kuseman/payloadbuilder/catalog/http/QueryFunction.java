package se.kuseman.payloadbuilder.catalog.http;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.io.entity.StringEntity;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** TVF for http query to be able to cross/outer apply http calls */
class QueryFunction extends TableFunctionInfo
{
    static final QualifiedName BODY = QualifiedName.of("body");
    private final CloseableHttpClient httpClient;
    private final List<IResponseTransformer> responseTransformers;

    QueryFunction(CloseableHttpClient httpClient, List<IResponseTransformer> responseTransformers)
    {
        super("query");
        this.httpClient = httpClient;
        this.responseTransformers = responseTransformers;
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context, List<IExpression> arguments, List<Option> options)
    {
        Map<String, Object> properties = new LinkedHashMap<>();
        String endpoint = getEndpoint(context, arguments.get(0));
        HttpUriRequestBase httpRequest = HttpDataSource.getRequestBase(context, options, endpoint);
        properties.put("Request", httpRequest.getMethod() + " " + httpRequest.getRequestUri());
        String body = getBody(context, options);
        properties.put("Body", body);
        return properties;
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, Optional<Schema> schema, List<IExpression> arguments, List<Option> options)
    {
        /*
         * @formatter:off
         * from http#query(`http://www.service.se/${countryCode}/items`) x with
         * (
         * method = 'POST',
         * body = `{ "country": "${@countryCode}" }`,
         * header."x-custom" = 'some header'
         * )
         * @formatter:on
         */

        String endpoint = getEndpoint(context, arguments.get(0));
        if (isBlank(endpoint))
        {
            throw new IllegalArgumentException("endpoint must be a non null string value");
        }

        HttpUriRequestBase httpRequest = HttpDataSource.getRequestBase(context, options, endpoint);

        String body = getBody(context, options);
        if (body != null)
        {
            httpRequest.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
        }

        return HttpDataSource.createIterator(httpClient, httpRequest, responseTransformers, context, options);
    }

    private String getEndpoint(IExecutionContext context, IExpression expression)
    {
        ValueVector vv = expression.eval(context);
        return vv.isNull(0) ? null
                : vv.getString(0)
                        .toString();
    }

    private String getBody(IExecutionContext context, List<Option> options)
    {
        IExpression expression = options.stream()
                .filter(o -> BODY.equalsIgnoreCase(o.getOption()))
                .map(o -> o.getValueExpression())
                .findAny()
                .orElse(null);
        if (expression == null)
        {
            return null;
        }

        ValueVector vv = expression.eval(context);
        return vv == null
                || vv.isNull(0) ? null
                        : vv.getString(0)
                                .toString();
    }
}
