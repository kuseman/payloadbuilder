package se.kuseman.payloadbuilder.catalog.http;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;

import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.http.io.entity.StringEntity;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
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
    static final String BODY = "body";
    private final List<IResponseTransformer> responseTransformers;

    QueryFunction(List<IResponseTransformer> responseTransformers)
    {
        super("query");
        this.responseTransformers = responseTransformers;
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, Optional<Schema> schema, List<IExpression> arguments, IDatasourceOptions options)
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

        ValueVector vv;

        vv = arguments.get(0)
                .eval(context);
        String endpoint = vv.isNull(0) ? null
                : vv.getString(0)
                        .toString();
        if (isBlank(endpoint))
        {
            throw new IllegalArgumentException("endpoint must be a non null string value");
        }

        vv = options.getOption(QualifiedName.of(HttpCatalog.METHOD), context);

        String method = vv == null
                || vv.isNull(0) ? HttpDataSource.GET
                        : vv.getString(0)
                                .toString()
                                .toLowerCase();

        vv = options.getOption(QualifiedName.of(BODY), context);

        String body = vv == null
                || vv.isNull(0) ? null
                        : vv.getString(0)
                                .toString();

        HttpUriRequestBase httpRequest = HttpDataSource.getRequestBase(method, endpoint);

        if (body != null)
        {
            httpRequest.setEntity(new StringEntity(body, StandardCharsets.UTF_8));
        }

        boolean failOnNon200 = true;

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
            else if (name.size() == 1
                    && HttpCatalog.FAIL_ON_NON_200.equalsIgnoreCase(name.getFirst()))
            {
                vv = option.getValueExpression()
                        .eval(context);
                failOnNon200 = vv.isNull(0) ? true
                        : vv.getBoolean(0);
            }
        }

        return HttpDataSource.execute(httpRequest, responseTransformers, failOnNon200);
    }
}
