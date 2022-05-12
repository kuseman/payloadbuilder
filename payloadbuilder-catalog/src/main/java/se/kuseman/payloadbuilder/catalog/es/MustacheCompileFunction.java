package se.kuseman.payloadbuilder.catalog.es;

import java.io.StringReader;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Compile mustable template */
class MustacheCompileFunction extends ScalarFunctionInfo
{
    private static final MustacheFactory FACTORY = new DefaultMustacheFactory();

    MustacheCompileFunction(Catalog catalog)
    {
        super(catalog, "mustachecompile");
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
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object templateObj = arguments.get(0)
                .eval(context);
        if (!(templateObj instanceof String))
        {
            throw new IllegalArgumentException("Expected template argument for " + getName() + " to return a String, got: " + templateObj);
        }
        Object paramsObj = arguments.get(1)
                .eval(context);
        if (!(paramsObj instanceof Map))
        {
            throw new IllegalArgumentException("Expected params argument for " + getName() + " to return a Map, got: " + paramsObj);
        }
        String template = (String) templateObj;
        @SuppressWarnings("unchecked")
        Map<String, Object> params = (Map<String, Object>) paramsObj;
        Mustache compiledTemplate = FACTORY.compile(new StringReader(template), "template");
        StringWriter writer = new StringWriter();
        compiledTemplate.execute(writer, params);
        return writer.toString();
    }
}
