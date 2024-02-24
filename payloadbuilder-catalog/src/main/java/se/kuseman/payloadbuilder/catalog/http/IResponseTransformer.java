package se.kuseman.payloadbuilder.catalog.http;

import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.http.ClassicHttpResponse;

import se.kuseman.payloadbuilder.api.execution.TupleIterator;

/** Transformer that transforms a http response into a tuple iterator */
public interface IResponseTransformer
{
    /** Returns true if this transformer can handle provided request/response. */
    boolean canHandle(HttpUriRequestBase request, ClassicHttpResponse response);

    /** Transform provided request/response to a {@link TupleIterator}. */
    TupleIterator transform(HttpUriRequestBase request, ClassicHttpResponse response);
}
