package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Collections.emptyMap;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Compile mustable template */
class MustacheCompileFunction extends ScalarFunctionInfo
{
    private static final MustacheFactory FACTORY = new DefaultMustacheFactory();

    MustacheCompileFunction(Catalog catalog)
    {
        super(catalog, "mustachecompile", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Compiles provided mustable template with provided params." + System.lineSeparator()
               + "Ex. mustachecompile(templateExpression, paramsExpression)"
               + System.lineSeparator()
               + "NOTE! paramsExpression should evaluate to a Map.";
    }

    @Override
    public int arity()
    {
        return 2;
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        final ValueVector value = arguments.get(0)
                .eval(input, context);
        final ValueVector args = arguments.get(1)
                .eval(input, context);

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.String);
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row);
            }

            @SuppressWarnings("unchecked")
            @Override
            public UTF8String getString(int row)
            {
                UTF8String template = value.getString(row);

                Object arg = args.valueAsObject(row);
                Map<String, Object> params = emptyMap();
                if (arg != null)
                {
                    if (!(arg instanceof Map))
                    {
                        throw new IllegalArgumentException("Expected params argument for " + getName() + " to return a Map, got: " + arg);
                    }
                    params = (Map<String, Object>) arg;
                }

                Mustache compiledTemplate = FACTORY.compile(new StringReader(template.toString()), "template");
                StringWriter writer = new StringWriter();
                compiledTemplate.execute(writer, params);
                return UTF8String.from(writer.toString());
            }

            @Override
            public Object getValue(int row)
            {
                throw new IllegalArgumentException("getValue should not be called on typed vectors");
            }
        };
    }
}
