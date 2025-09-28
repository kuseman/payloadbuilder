package se.kuseman.payloadbuilder.catalog.http;

import java.io.IOException;
import java.util.List;

import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.http.ClassicHttpResponse;

import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;

/** Transformer that transforms a http response into a tuple iterator */
public interface IResponseTransformer
{
    /** Returns true if this transformer can handle provided request/response. */
    boolean canHandle(HttpUriRequestBase request, ClassicHttpResponse response);

    /** Transform provided request/response to a {@link TupleIterator}. This method is responsible of closing the stream when complete. */
    TupleIterator transform(HttpUriRequestBase request, ClassicHttpResponse response, IExecutionContext context, List<Option> options) throws IOException;
}
