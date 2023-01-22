package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactoryBuilder;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.message.BasicHeader;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.Timeout;

import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.catalog.CredentialsException;

/** Client utils */
public final class HttpClientUtils
{
    static final int DEFAULT_RECIEVE_TIMEOUT = 15000;
    static final int DEFAULT_CONNECT_TIMEOUT = 500;
    private static final List<Header> HEADERS = asList(new BasicHeader(HttpHeaders.ACCEPT_ENCODING, "gzip"), new BasicHeader(HttpHeaders.ACCEPT, "application/json"),
            new BasicHeader(HttpHeaders.CONTENT_TYPE, "application/json"));

    /**
     * Execute request. Handling 401 authorization etc. NOTE! Caller should take care of resource handling of the response
     *
     * @throws IOException Throws IOException
     */
    public static <T> T execute(String catalogAlias, ClassicHttpRequest request, Boolean trustCertificate, Integer connectTimeout, Integer recieveTimeout, AuthType authType, String authUsername,
            Object authPassword, HttpClientResponseHandler<T> handler) throws IOException
    {
        CloseableHttpClient client = getClient(catalogAlias, trustCertificate, connectTimeout, recieveTimeout, authType, authUsername, getPassword(authPassword));

        return client.execute(request, new HttpClientResponseHandler<T>()
        {
            @Override
            public T handleResponse(ClassicHttpResponse response) throws HttpException, IOException
            {
                if (response.getCode() == HttpStatus.SC_UNAUTHORIZED)
                {
                    throw new CredentialsException(catalogAlias, "Invalid username/password in catalog properties" + (catalogAlias != null ? (" for " + catalogAlias)
                            : ""));
                }
                return handler.handleResponse(response);
            }
        });
    }

    /**
     * Execute request with provided session.
     *
     * @throws IOException Throws IOException
     */
    static <T> T execute(IQuerySession session, String catalogAlias, ClassicHttpRequest request, HttpClientResponseHandler<T> handler) throws IOException
    {
        Boolean trustCertificate = (Boolean) session.getCatalogProperty(catalogAlias, ESCatalog.TRUSTCERTIFICATE_KEY)
                .valueAsObject(0);
        AuthType authType = AuthType.from(session.getCatalogProperty(catalogAlias, ESCatalog.AUTH_TYPE_KEY)
                .valueAsString(0));
        String authUsername = session.getCatalogProperty(catalogAlias, ESCatalog.AUTH_USERNAME_KEY)
                .valueAsString(0);
        String authPassword = getPassword(session.getCatalogProperty(catalogAlias, ESCatalog.AUTH_PASSWORD_KEY)
                .valueAsObject(0));
        Integer connectTimeout = (Integer) session.getCatalogProperty(catalogAlias, ESCatalog.CONNECT_TIMEOUT_KEY)
                .valueAsObject(0);
        Integer receiveTimeout = (Integer) session.getCatalogProperty(catalogAlias, ESCatalog.RECEIVE_TIMEOUT_KEY)
                .valueAsObject(0);
        return execute(catalogAlias, request, trustCertificate, connectTimeout, receiveTimeout, authType, authUsername, authPassword, handler);
    }

    /**
     * Create a client from provided components
     */
    private static CloseableHttpClient getClient(String catalogAlias, Boolean trustCertificate, Integer connectTimeout, Integer recieveTimeout, AuthType authType, String authUsername,
            String authPassword)
    {
        PoolingHttpClientConnectionManagerBuilder manager = PoolingHttpClientConnectionManagerBuilder.create()
                .setDefaultConnectionConfig(ConnectionConfig.custom()
                        .setConnectTimeout(Timeout.ofMilliseconds(connectTimeout == null ? DEFAULT_CONNECT_TIMEOUT
                                : connectTimeout))
                        .setSocketTimeout(Timeout.ofMilliseconds(recieveTimeout == null ? DEFAULT_RECIEVE_TIMEOUT
                                : recieveTimeout))
                        .build());

        if (trustCertificate != null
                && trustCertificate)
        {
            try
            {
                manager.setSSLSocketFactory(SSLConnectionSocketFactoryBuilder.create()
                        .setSslContext(SSLContextBuilder.create()
                                .loadTrustMaterial(TrustAllStrategy.INSTANCE)
                                .build())
                        .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                        .build());
            }
            catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e)
            {
                throw new RuntimeException("Cannot create HttpClient", e);
            }
        }

        HttpClientBuilder builder = HttpClients.custom()
                .setDefaultHeaders(HEADERS)
                .disableCookieManagement()
                .setConnectionManager(manager.build())
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setContentCompressionEnabled(true)
                        .build());

        if (authType != null
                && authType != AuthType.NONE)
        {
            switch (authType)
            {
                case BASIC:
                    if (isBlank(authUsername)
                            || isBlank(authPassword))
                    {
                        throw new CredentialsException(catalogAlias, "Missing username/password in catalog properties" + (catalogAlias != null ? (" for " + catalogAlias)
                                : ""));
                    }

                    BasicCredentialsProvider provider = new BasicCredentialsProvider();
                    UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(authUsername, authPassword.toCharArray());
                    provider.setCredentials(new AuthScope(null, null, -1, null, null), credentials);
                    builder.setDefaultCredentialsProvider(provider);
                    break;
                default:
                    throw new RuntimeException("Unsupported auth type " + authType);
            }
        }

        return builder.build();
    }

    private static String getPassword(Object password)
    {
        if (password instanceof String)
        {
            return (String) password;
        }
        else if (password instanceof char[])
        {
            return new String((char[]) password);
        }
        return null;
    }
}
