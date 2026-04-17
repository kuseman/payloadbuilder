package se.kuseman.payloadbuilder.catalog.es;

import java.io.IOException;

import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;

import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.catalog.AuthType;
import se.kuseman.payloadbuilder.catalog.HttpClientUtils;

/** ES-specific HTTP client utilities that delegate to the shared {@link se.kuseman.payloadbuilder.catalog.HttpClientUtils} */
final class ESHttpClientUtils
{
    private ESHttpClientUtils()
    {
    }

    /**
     * Execute request with provided session, reading ES-specific catalog properties.
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
        String authPassword = se.kuseman.payloadbuilder.catalog.HttpClientUtils.getPassword(session.getCatalogProperty(catalogAlias, ESCatalog.AUTH_PASSWORD_KEY)
                .valueAsObject(0));
        Integer connectTimeout = (Integer) session.getCatalogProperty(catalogAlias, ESCatalog.CONNECT_TIMEOUT_KEY)
                .valueAsObject(0);
        Integer receiveTimeout = (Integer) session.getCatalogProperty(catalogAlias, ESCatalog.RECEIVE_TIMEOUT_KEY)
                .valueAsObject(0);
        return HttpClientUtils.execute(catalogAlias, request, trustCertificate, connectTimeout, receiveTimeout, authType, authUsername, authPassword, handler);
    }
}
