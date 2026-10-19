package se.kuseman.payloadbuilder.catalog.http;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.TrustAllStrategy;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

/** Provider of http clients. Holds a default client and a lazy created client that trusts all certificates */
class HttpClientProvider implements AutoCloseable
{
    private final CloseableHttpClient httpClient;
    private volatile CloseableHttpClient trustAllHttpClient;

    HttpClientProvider()
    {
        this.httpClient = createClient(false);
    }

    /** Return client. If trustCertificate is true a client that trusts all certificates and skips host name verification is returned */
    CloseableHttpClient get(boolean trustCertificate)
    {
        if (!trustCertificate)
        {
            return httpClient;
        }

        CloseableHttpClient client = trustAllHttpClient;
        if (client == null)
        {
            synchronized (this)
            {
                client = trustAllHttpClient;
                if (client == null)
                {
                    client = createClient(true);
                    trustAllHttpClient = client;
                }
            }
        }
        return client;
    }

    @Override
    public void close()
    {
        closeQuietly(httpClient);
        closeQuietly(trustAllHttpClient);
    }

    private static void closeQuietly(CloseableHttpClient client)
    {
        if (client == null)
        {
            return;
        }
        try
        {
            client.close();
        }
        catch (IOException e)
        {
        }
    }

    private static CloseableHttpClient createClient(boolean trustCertificate)
    {
        PoolingHttpClientConnectionManagerBuilder manager = PoolingHttpClientConnectionManagerBuilder.create()
                .setDefaultConnectionConfig(ConnectionConfig.custom()
                        .setConnectTimeout(Timeout.ofMilliseconds(HttpCatalog.DEFAULT_CONNECT_TIMEOUT))
                        .setSocketTimeout(Timeout.ofMilliseconds(HttpCatalog.DEFAULT_RECIEVE_TIMEOUT))
                        .build());

        if (trustCertificate)
        {
            try
            {
                manager.setTlsSocketStrategy(ClientTlsStrategyBuilder.create()
                        .setSslContext(SSLContextBuilder.create()
                                .loadTrustMaterial(TrustAllStrategy.INSTANCE)
                                .build())
                        .setHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                        .buildClassic());
            }
            catch (KeyManagementException | NoSuchAlgorithmException | KeyStoreException e)
            {
                throw new RuntimeException("Cannot create HttpClient", e);
            }
        }

        return HttpClients.custom()
                .setConnectionManager(manager.build())
                .evictExpiredConnections()
                .evictIdleConnections(TimeValue.ofSeconds(60))
                .build();
    }
}
