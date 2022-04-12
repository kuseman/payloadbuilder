package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicHeader;

import se.kuseman.payloadbuilder.api.session.IQuerySession;
import se.kuseman.payloadbuilder.catalog.CredentialsException;

/** Client utils */
final class HttpClientUtils
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
    static CloseableHttpResponse execute(String catalogAlias, HttpRequestBase request, Boolean trustCertificate, Integer connectTimeout, Integer recieveTimeout, AuthType authType, String authUsername,
            Object authPassword) throws IOException
    {
        CloseableHttpClient client = getClient(catalogAlias, trustCertificate, connectTimeout, recieveTimeout, authType, authUsername, getPassword(authPassword));
        CloseableHttpResponse response = client.execute(request);
        if (response.getStatusLine()
                .getStatusCode() == HttpStatus.SC_UNAUTHORIZED)
        {
            throw new CredentialsException(catalogAlias, "Invalid username/password in catalog properties" + (catalogAlias != null ? (" for " + catalogAlias)
                    : ""));
        }
        return response;
    }

    /**
     * Execute request with provided session.
     *
     * @throws IOException Throws IOException
     */
    static CloseableHttpResponse execute(IQuerySession session, String catalogAlias, HttpRequestBase request) throws IOException
    {
        Boolean trustCertificate = session.getCatalogProperty(catalogAlias, ESCatalog.TRUSTCERTIFICATE_KEY);
        AuthType authType = AuthType.from(session.getCatalogProperty(catalogAlias, ESCatalog.AUTH_TYPE_KEY));
        String authUsername = session.getCatalogProperty(catalogAlias, ESCatalog.AUTH_USERNAME_KEY);
        String authPassword = getPassword(session.getCatalogProperty(catalogAlias, ESCatalog.AUTH_PASSWORD_KEY));
        Integer connectTimeout = session.getCatalogProperty(catalogAlias, ESCatalog.CONNECT_TIMEOUT_KEY);
        Integer receiveTimeout = session.getCatalogProperty(catalogAlias, ESCatalog.RECEIVE_TIMEOUT_KEY);
        return execute(catalogAlias, request, trustCertificate, connectTimeout, receiveTimeout, authType, authUsername, authPassword);
    }

    /**
     * Create a client from provided components
     *
     * @param basicAuthPasswordObj Is either a password string or a char[]
     */
    private static CloseableHttpClient getClient(String catalogAlias, Boolean trustCertificate, Integer connectTimeout, Integer recieveTimeout, AuthType authType, String authUsername,
            String authPassword)
    {
        HttpClientBuilder builder = HttpClients.custom()
                .setDefaultHeaders(HEADERS)
                .disableCookieManagement()
                .setDefaultRequestConfig(RequestConfig.custom()
                        .setContentCompressionEnabled(true)
                        .setConnectTimeout(connectTimeout == null ? DEFAULT_CONNECT_TIMEOUT
                                : connectTimeout)
                        .setSocketTimeout(recieveTimeout == null ? DEFAULT_RECIEVE_TIMEOUT
                                : recieveTimeout)
                        .build());

        if (authType != AuthType.NONE)
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

                    CredentialsProvider provider = new BasicCredentialsProvider();
                    UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(authUsername, authPassword);
                    provider.setCredentials(AuthScope.ANY, credentials);
                    builder.setDefaultCredentialsProvider(provider);
                    break;
                default:
                    throw new RuntimeException("Unsupported auth type " + authType);
            }
        }

        if (trustCertificate != null
                && trustCertificate)
        {
            builder.setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE);
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

    /** Auth type */
    enum AuthType
    {
        NONE,
        BASIC;

        static AuthType from(Object value)
        {
            if (value == null)
            {
                return NONE;
            }
            return AuthType.valueOf(StringUtils.upperCase(String.valueOf(value)));
        }
    }
}
