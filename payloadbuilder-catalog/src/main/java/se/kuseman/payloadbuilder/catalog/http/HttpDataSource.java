package se.kuseman.payloadbuilder.catalog.http;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.io.IOException;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.RequestFailedException;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.util.Timeout;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.ISeekKey;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.catalog.http.HttpCatalog.Request;

/** Data source impl. for Http catalog */
@SuppressWarnings("deprecation")
class HttpDataSource implements IDatasource
{
    static final String GET = "GET";
    private static final IResponseTransformer FALLBACK_TRANSFORMER = new FallbackResponseTransformer();
    private final CloseableHttpClient httpClient;
    private final String catalogAlias;
    private final String endpoint;
    private final ISeekPredicate seekPredicate;
    private final Request request;
    private final List<IResponseTransformer> responseTransformers;

    HttpDataSource(CloseableHttpClient httpClient, String catalogAlias, String endpoint, ISeekPredicate seekPredicate, Request request, List<IResponseTransformer> responseTransformers)
    {
        this.httpClient = requireNonNull(httpClient);
        this.catalogAlias = requireNonNull(catalogAlias);
        this.endpoint = requireNonNull(endpoint);
        this.seekPredicate = seekPredicate;
        this.request = requireNonNull(request);
        this.responseTransformers = requireNonNull(responseTransformers);
    }

    @Override
    public TupleIterator execute(IExecutionContext context, IDatasourceOptions options)
    {
        String endpoint = this.endpoint;
        if (!StringUtils.startsWithIgnoreCase(endpoint, "http"))
        {
            endpoint = context.getSession()
                    .getCatalogProperty(catalogAlias, endpoint)
                    .valueAsString(0);
            if (StringUtils.isAllBlank(endpoint))
            {
                throw new IllegalArgumentException("Missing catalog property '" + this.endpoint + "' for catalog alias: " + catalogAlias);
            }
        }

        HttpUriRequestBase request = getBaseRequest(context, options, endpoint);
        return execute(httpClient, request, responseTransformers, context, options);
    }

    static TupleIterator execute(CloseableHttpClient httpClient, HttpUriRequestBase request, List<IResponseTransformer> responseTransformers, IExecutionContext context, IDatasourceOptions options)
    {
        ValueVector vv = options.getOption(QualifiedName.of(HttpCatalog.FAIL_ON_NON_200), context);
        boolean failOnNon200 = vv == null
                || vv.isNull(0) ? true
                        : vv.getBoolean(0);

        vv = options.getOption(QualifiedName.of(HttpCatalog.CONNECT_TIMEOUT), context);
        int connectTimeout = vv == null
                || vv.isNull(0) ? HttpCatalog.DEFAULT_CONNECT_TIMEOUT
                        : vv.getInt(0);

        vv = options.getOption(QualifiedName.of(HttpCatalog.RECEIVE_TIMEOUT), context);
        int receiveTimeout = vv == null
                || vv.isNull(0) ? HttpCatalog.DEFAULT_RECIEVE_TIMEOUT
                        : vv.getInt(0);

        if (connectTimeout != HttpCatalog.DEFAULT_CONNECT_TIMEOUT
                || receiveTimeout != HttpCatalog.DEFAULT_RECIEVE_TIMEOUT)
        {
            RequestConfig config = RequestConfig.custom()
                    .setResponseTimeout(Timeout.ofMilliseconds(receiveTimeout))
                    .setConnectTimeout(Timeout.ofMilliseconds(connectTimeout))
                    .build();

            request.setConfig(config);
        }

        Runnable abortListener = () ->
        {
            try
            {
                request.abort();
            }
            catch (UnsupportedOperationException e)
            {
            }
        };

        context.getSession()
                .registerAbortListener(abortListener);

        ClassicHttpResponse response = null;
        try
        {
            response = httpClient.execute(request);
            if (response.getCode() != 200)
            {
                if (!failOnNon200)
                {
                    EntityUtils.consumeQuietly(response.getEntity());
                    return TupleIterator.EMPTY;
                }

                String error = IOUtils.toString(response.getEntity()
                        .getContent(), StandardCharsets.UTF_8);
                EntityUtils.consumeQuietly(response.getEntity());
                throw new RuntimeException("Response was non 200. Code: " + response.getCode() + ", body: " + error);
            }

            for (IResponseTransformer transformer : responseTransformers)
            {
                if (transformer.canHandle(request, response))
                {
                    return transformer.transform(request, response, context, options);
                }
            }

            return FALLBACK_TRANSFORMER.transform(request, response, context, options);
        }
        catch (IOException e)
        {
            if (response != null)
            {
                EntityUtils.consumeQuietly(response.getEntity());
            }
            if (!failOnNon200
                    || (e instanceof RequestFailedException ex
                            && ex.getMessage()
                                    .contains("Request aborted")))
            {
                return TupleIterator.EMPTY;
            }

            throw new RuntimeException("Error performing HTTP request", e);
        }
        finally
        {
            context.getSession()
                    .unregisterAbortListener(abortListener);
        }
    }

    private HttpUriRequestBase getBaseRequest(IExecutionContext context, IDatasourceOptions options, String baseEndpoint)
    {
        String endpoint = baseEndpoint;

        // Gather place holder values
        Map<String, List<Object>> replaceValues = new HashMap<>();

        // Predicates
        if (!request.predicates()
                .isEmpty())
        {
            for (HttpCatalog.Predicate p : request.predicates())
            {
                replaceValues.put(p.name(), evalPredicate(context, p.values()));
            }
        }

        // .. seek predicate
        if (seekPredicate != null)
        {
            List<ISeekKey> keys = seekPredicate.getSeekKeys(context);
            int size = keys.size();
            for (int i = 0; i < size; i++)
            {
                ISeekKey key = keys.get(i);
                String column = seekPredicate.getIndexColumns()
                        .get(i);
                List<Object> values = new ArrayList<>();
                ValueVector vectorValues = key.getValue();
                int vsize = vectorValues.size();
                for (int v = 0; v < vsize; v++)
                {
                    Object value = vectorValues.valueAsObject(v);
                    values.add(value);
                }

                replaceValues.compute(column, (k, v) ->
                {
                    if (v == null)
                    {
                        return values;
                    }

                    v.addAll(values);
                    return v;
                });
            }
        }

        // Replace query expression
        if (request.queryExpression() != null)
        {
            String queryPart = request.queryExpression()
                    .eval(context)
                    .valueAsString(0);

            if (!replaceValues.isEmpty())
            {
                StringBuilder sb = new StringBuilder(queryPart.length() * 2);
                Matcher matcher = HttpCatalog.PLACEHOLDER_PATTERN.matcher(queryPart);
                while (matcher.find())
                {
                    List<Object> list = replaceValues.get(matcher.group(1));
                    String replaceValue = list.stream()
                            .filter(Objects::nonNull)
                            .map(String::valueOf)
                            .map(v -> URLEncoder.encode(v, StandardCharsets.UTF_8))
                            .collect(joining(","));
                    matcher.appendReplacement(sb, replaceValue);
                }
                matcher.appendTail(sb);
                endpoint += sb.toString();
            }
        }

        HttpUriRequestBase httpRequest = getRequestBase(context, options, endpoint);

        // Process body expression
        if (request.bodyExpression() != null)
        {
            ContentType contentType = ContentType.APPLICATION_JSON;
            if (request.contentType() != null)
            {
                contentType = ContentType.parseLenient(request.contentType()
                        .eval(context)
                        .valueAsString(0));
            }
            // TODO: request body builder abstraction, escape strings, append items
            // ie.
            // xml -> <field>value1</field><field>value2</field>
            // json -> "value1","value2"
            // For now we only support json
            if (!ContentType.APPLICATION_JSON.isSameMimeType(contentType))
            {
                throw new IllegalArgumentException("Only application/json Content-Type is supported");
            }

            String body = request.bodyExpression()
                    .eval(context)
                    .valueAsString(0);

            if (!replaceValues.isEmpty())
            {
                StringBuilder sb = new StringBuilder(body.length() * 2);
                Matcher matcher = HttpCatalog.PLACEHOLDER_PATTERN.matcher(body);
                while (matcher.find())
                {
                    List<Object> list = replaceValues.get(matcher.group(1));
                    String replaceValue = list.stream()
                            .filter(Objects::nonNull)
                            .map(v ->
                            {
                                if (v instanceof Number)
                                {
                                    return String.valueOf(v);
                                }

                                return "\"" + StringEscapeUtils.escapeJson(String.valueOf(v))
                                        .replace("\\", "\\\\")
                                       + "\"";
                            })
                            .collect(joining(","));
                    matcher.appendReplacement(sb, replaceValue);
                }
                matcher.appendTail(sb);
                body = sb.toString();
            }

            httpRequest.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
        }

        return httpRequest;
    }

    static HttpUriRequestBase getRequestBase(IExecutionContext context, IDatasourceOptions options, String endpoint)
    {
        ValueVector vv = options.getOption(HttpCatalog.METHOD, context);
        String method = vv == null
                || vv.isNull(0) ? GET
                        : vv.getString(0)
                                .toString()
                                .toUpperCase();

        HttpUriRequestBase httpRequest = new HttpUriRequestBase(method, URI.create(endpoint));
        for (Option option : options.getOptions())
        {
            QualifiedName name = option.getOption();
            if (name.size() == 2
                    && HttpCatalog.HEADER.equalsIgnoreCase(name.getFirst()))
            {
                String value = option.getValueExpression()
                        .eval(context)
                        .valueAsString(0);
                httpRequest.addHeader(name.getParts()
                        .get(1), value);
            }
        }

        return httpRequest;
    }

    private List<Object> evalPredicate(IExecutionContext context, List<IExpression> expressions)
    {
        return expressions.stream()
                .map(e -> e.eval(context)
                        .valueAsObject(0))
                .toList();
    }
}
