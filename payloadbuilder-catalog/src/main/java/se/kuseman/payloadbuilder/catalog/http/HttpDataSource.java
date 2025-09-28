package se.kuseman.payloadbuilder.catalog.http;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.RequestFailedException;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.net.URIAuthority;
import org.apache.hc.core5.net.URIBuilder;
import org.apache.hc.core5.util.Timeout;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
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
    private final List<Option> options;

    HttpDataSource(CloseableHttpClient httpClient, String catalogAlias, String endpoint, ISeekPredicate seekPredicate, Request request, List<IResponseTransformer> responseTransformers,
            List<Option> options)
    {
        this.httpClient = requireNonNull(httpClient);
        this.catalogAlias = requireNonNull(catalogAlias);
        this.endpoint = requireNonNull(endpoint);
        this.seekPredicate = seekPredicate;
        this.request = requireNonNull(request);
        this.responseTransformers = requireNonNull(responseTransformers);
        this.options = options;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        HttpUriRequestBase request = getRequest(context, false);
        return createIterator(httpClient, request, responseTransformers, context, options);
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> properties = new LinkedHashMap<>();
        HttpUriRequestBase httpRequest = getRequest(context, true);
        properties.put("Request", httpRequest.getMethod() + " " + httpRequest.getRequestUri());
        if (httpRequest.getEntity() instanceof StringEntity se)
        {
            String body;
            try
            {
                body = IOUtils.toString(se.getContent(), StandardCharsets.UTF_8);
                properties.put("Body", body);
            }
            catch (IOException e)
            {
            }
        }
        return properties;
    }

    private String getBaseEndpoint(IExecutionContext context)
    {
        String endpoint = this.endpoint;
        if (!Strings.CI.startsWith(endpoint, "http"))
        {
            endpoint = context.getSession()
                    .getCatalogProperty(catalogAlias, endpoint)
                    .valueAsString(0);
            if (StringUtils.isAllBlank(endpoint))
            {
                throw new IllegalArgumentException("Missing catalog property '" + this.endpoint + "' for catalog alias: " + catalogAlias);
            }
        }
        return endpoint;
    }

    /** Builds the request to be made out of query and body expressions. */
    private HttpUriRequestBase getRequest(IExecutionContext context, boolean describe)
    {
        String endpoint = getBaseEndpoint(context);

        Map<String, Object> values = emptyMap();
        if (!request.predicates()
                .isEmpty()
                || seekPredicate != null)
        {
            values = PatternUtils.createValues(context, seekPredicate, request.predicates(), describe);
        }

        // Replace query expression
        if (request.queryExpression() != null)
        {
            String query = request.queryExpression()
                    .eval(context)
                    .valueAsString(0);

            endpoint += PatternUtils.replacePattern(query, PatternUtils.URL_ENCODED_CONTENT_TYPE, values);
        }

        HttpUriRequestBase httpRequest = getRequestBase(context, options, endpoint);

        // Process body expression
        if (request.bodyExpression() != null)
        {
            String body = request.bodyExpression()
                    .eval(context)
                    .valueAsString(0);

            // Default to application/json, to be backwards compatible with pre. mustache
            ContentType contentType = ContentType.APPLICATION_JSON;
            if (request.contentType() != null)
            {
                contentType = ContentType.parseLenient(request.contentType()
                        .eval(context)
                        .valueAsString(0));
            }
            body = PatternUtils.replacePattern(body, contentType, values);
            httpRequest.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
        }

        return httpRequest;
    }

    /** Construct a base request with method and headers. */
    static HttpUriRequestBase getRequestBase(IExecutionContext context, List<Option> options, String endpoint)
    {
        String method = "GET";
        List<Header> headers = new ArrayList<>();

        for (Option option : options)
        {
            QualifiedName name = option.getOption();
            if (name.size() == 2
                    && HttpCatalog.HEADER.equalsIgnoreCase(name.getFirst()))
            {
                String value = option.getValueExpression()
                        .eval(context)
                        .valueAsString(0);
                headers.add(new BasicHeader(name.getParts()
                        .get(1), value));
            }
            else if (HttpCatalog.METHOD.equalsIgnoreCase(name))
            {
                String value = option.getValueExpression()
                        .eval(context)
                        .valueAsString(0);
                method = value != null ? value.toUpperCase()
                        : method;
            }
        }

        HttpUriRequestBase httpRequest = new HttpUriRequestBase(method, getURI(endpoint));
        httpRequest.setHeaders(headers.toArray(new Header[0]));
        return httpRequest;
    }

    /** Creates an {@link TupleIterator} and actual do the http request work. */
    static TupleIterator createIterator(CloseableHttpClient httpClient, HttpUriRequestBase request, List<IResponseTransformer> responseTransformers, IExecutionContext context, List<Option> options)
    {
        ValueVector vv = context.getOption(QualifiedName.of(HttpCatalog.FAIL_ON_NON_200), options);
        boolean failOnNon200 = vv == null
                || vv.isNull(0) ? true
                        : vv.getBoolean(0);

        vv = context.getOption(QualifiedName.of(HttpCatalog.CONNECT_TIMEOUT), options);
        int connectTimeout = vv == null
                || vv.isNull(0) ? HttpCatalog.DEFAULT_CONNECT_TIMEOUT
                        : vv.getInt(0);

        vv = context.getOption(QualifiedName.of(HttpCatalog.RECEIVE_TIMEOUT), options);
        int receiveTimeout = vv == null
                || vv.isNull(0) ? HttpCatalog.DEFAULT_RECIEVE_TIMEOUT
                        : vv.getInt(0);

        vv = context.getOption(QualifiedName.of(HttpCatalog.PRINT_HEADERS), options);
        boolean printHeaders = vv == null
                || vv.isNull(0) ? false
                        : vv.getBoolean(0);

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
            if (printHeaders)
            {
                for (Header header : response.getHeaders())
                {
                    context.getSession()
                            .getPrintWriter()
                            .append(header.getName())
                            .append(':')
                            .append(' ')
                            .append(header.getValue())
                            .append('\n');
                }
            }

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
        catch (RuntimeException e)
        {
            throw e;
        }
        catch (Exception e)
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

    /** Dummy handler to ensure URL to not do any stupid connections we don't want. */
    static final URLStreamHandler DUMMY = new URLStreamHandler()
    {
        @Override
        protected URLConnection openConnection(URL u) throws IOException
        {
            return null;
        }
    };

    static URI getURI(String endpoint)
    {
        try
        {
            // We first use URL to parse the endpoint since that wont throw URISyntaxException
            // and then we use URIBuilder to construct a friendly URI
            URL url = new URL(null, endpoint, DUMMY);
            return new URIBuilder().setCharset(StandardCharsets.UTF_8)
                    .setAuthority(URIAuthority.create(url.getAuthority()))
                    .setScheme(url.getProtocol())
                    .setPort(url.getPort())
                    .setPath(url.getPath())
                    .setCustomQuery(url.getQuery())
                    .setFragment(url.getRef())
                    .build();
        }
        catch (MalformedURLException | URISyntaxException e)
        {
            throw new RuntimeException(e);
        }
    }
}
