package se.kuseman.payloadbuilder.catalog.http;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.IOException;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringEscapeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.apache.hc.core5.http.ContentType;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.mustachejava.Code;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.DefaultMustacheVisitor;
import com.github.mustachejava.Iteration;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheException;
import com.github.mustachejava.MustacheVisitor;
import com.github.mustachejava.TemplateContext;
import com.github.mustachejava.codes.IterableCode;
import com.github.mustachejava.codes.NotIterableCode;
import com.github.mustachejava.codes.ValueCode;
import com.github.mustachejava.reflect.ReflectionObjectHandler;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.ISeekKey;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.catalog.http.HttpCatalog.Predicate;

/** Utils class for handling body patterns in HttpCatalog. */
class PatternUtils
{
    static final String SEEK_VALUES = "@seekValues";
    /** Special content type used when query part is processed */
    static final ContentType URL_ENCODED_CONTENT_TYPE = ContentType.create("URL_ENCODED", "UTF8");

    /**
     * Extension to reflection object handler that inserts a iterHasNext flag for all iterations to know when there are more items etc. This to overcome hurdle in mustache with commas etc.
     */
    private static class PlbReflectionObjectHandler extends ReflectionObjectHandler
    {
        private static final String ITER_HAS_NEXT = "iterHasNext";

        @SuppressWarnings("rawtypes")
        @Override
        public Writer iterate(Iteration iteration, Writer writer, Object object, List<Object> scopes)
        {
            if (object == null)
            {
                return writer;
            }
            if (object instanceof Boolean)
            {
                if (!(Boolean) object)
                {
                    return writer;
                }
            }
            if (object instanceof String)
            {
                if (object.toString()
                        .equals(""))
                {
                    return writer;
                }
            }
            if (object instanceof List)
            {
                List list = (List) object;
                int length = list.size();

                Map<String, Object> iter = new HashMap<>();
                scopes.add(iter);

                for (int i = 0; i < length; i++)
                {
                    iter.put(ITER_HAS_NEXT, i < length - 1);
                    writer = iteration.next(writer, coerce(list.get(i)), scopes);
                }

                scopes.remove(iter);
            }
            else if (object instanceof Iterable iterable)
            {
                Map<String, Object> iter = new HashMap<>();
                scopes.add(iter);

                Iterator iterator = iterable.iterator();
                while (iterator.hasNext())
                {
                    Object next = iterator.next();
                    // NOTE! Need to call hasNext AFTER next
                    iter.put(ITER_HAS_NEXT, iterator.hasNext());
                    writer = iteration.next(writer, coerce(next), scopes);
                }

                scopes.remove(iter);
            }
            else if (object instanceof Iterator)
            {
                Map<String, Object> iter = new HashMap<>();
                scopes.add(iter);

                Iterator iterator = (Iterator) object;
                while (iterator.hasNext())
                {
                    Object next = iterator.next();
                    // NOTE! Need to call hasNext AFTER next
                    iter.put(ITER_HAS_NEXT, iterator.hasNext());
                    writer = iteration.next(writer, coerce(next), scopes);
                }

                scopes.remove(iter);
            }
            else if (object.getClass()
                    .isArray())
            {
                Map<String, Object> iter = new HashMap<>();
                scopes.add(iter);

                int length = Array.getLength(object);
                for (int i = 0; i < length; i++)
                {
                    iter.put(ITER_HAS_NEXT, i < length - 1);
                    writer = iteration.next(writer, coerce(Array.get(object, i)), scopes);
                }

                scopes.remove(iter);
            }
            else
            {
                writer = iteration.next(writer, object, scopes);
            }
            return writer;
        }
    }

    /** A custom mustache factory to handle escaping and striping delimiters of iterators etc. */
    private static class PlbMustacheFactory extends DefaultMustacheFactory
    {
        static final PlbMustacheFactory JSON_ENCODING_FACTORY = new PlbMustacheFactory(ContentType.APPLICATION_JSON);
        static final PlbMustacheFactory TEXT_ENCODING_FACTORY = new PlbMustacheFactory(ContentType.TEXT_PLAIN);
        static final PlbMustacheFactory URL_ENCODING_FACTORY = new PlbMustacheFactory(URL_ENCODED_CONTENT_TYPE);
        private final ContentType contentType;

        PlbMustacheFactory(ContentType contentType)
        {
            this.contentType = contentType;
            setObjectHandler(new PlbReflectionObjectHandler());
        }

        @Override
        public MustacheVisitor createMustacheVisitor()
        {
            return new DefaultMustacheVisitor(this)
            {
                /** Helper functions. If variable ends with help function then return a customized code. */
                @Override
                public void value(TemplateContext tc, String variable, boolean encoded)
                {
                    if (JoinCode.match(variable))
                    {
                        list.add(JoinCode.create(tc, df, variable, contentType));
                    }
                    else if (ToJsonArrayCode.match(variable))
                    {
                        list.add(ToJsonArrayCode.create(tc, df, variable));
                    }
                    else
                    {
                        list.add(new ValueCode(tc, df, variable, encoded)
                        {
                            @Override
                            public Writer execute(Writer writer, List<Object> scopes)
                            {
                                if (ContentType.APPLICATION_JSON.isSameMimeType(contentType))
                                {
                                    Object object = get(scopes);
                                    // When rendering json and we are about to stringify an Iterable we build a comma separated list with escaped json values.
                                    // Ie.
                                    // pattern: {{id}}
                                    // model: id = ["some", "\"values"]
                                    // then we render this as: "some", "\\"values"
                                    // This to handle previous pattern version of HttpCatalog
                                    if (object instanceof Iterable<?> it)
                                    {
                                        handleJsonIterable(writer, it);
                                        return appendText(run(writer, scopes));
                                    }
                                }
                                else if (URL_ENCODED_CONTENT_TYPE.isSameMimeType(contentType))
                                {
                                    Object object = get(scopes);
                                    // When this is a rendering of query part we join the values and then urlencode each item
                                    // This to handle previous pattern version of HttpCatalog
                                    if (object instanceof Iterable<?> it)
                                    {
                                        handleUrlEncodedIterable(writer, it);
                                        return appendText(run(writer, scopes));
                                    }
                                }
                                return super.execute(writer, scopes);
                            }
                        });
                    }
                }

                private void handleUrlEncodedIterable(Writer writer, Iterable<?> it)
                {
                    StringBuilder sb = new StringBuilder();
                    boolean first = true;
                    for (Object o : it)
                    {
                        if (!first)
                        {
                            sb.append(',');
                        }
                        sb.append(PlbMustacheFactory.encode(String.valueOf(o), contentType));
                        first = false;
                    }

                    PlbMustacheFactory.write(sb.toString(), writer, "Failed to render JSON iterable value: ");
                }

                private void handleJsonIterable(Writer writer, Iterable<?> it)
                {
                    StringBuilder sb = new StringBuilder();
                    boolean first = true;
                    for (Object o : it)
                    {
                        if (!first)
                        {
                            sb.append(',');
                        }
                        if (o instanceof UTF8String s)
                        {
                            o = s.toString();
                        }
                        boolean quote = o instanceof CharSequence;
                        if (quote)
                        {
                            sb.append('"');
                        }
                        sb.append(PlbMustacheFactory.encode(String.valueOf(o), contentType));
                        if (quote)
                        {
                            sb.append('"');
                        }
                        first = false;
                    }

                    PlbMustacheFactory.write(sb.toString(), writer, "Failed to render JSON iterable value: ");
                }
            };
        }

        @Override
        public void encode(String value, Writer writer)
        {
            write(encode(value, contentType), writer, "Failed to encode value: ");
        }

        @SuppressWarnings("deprecation")
        static String encode(String input, ContentType contentType)
        {
            if (ContentType.APPLICATION_JSON.isSameMimeType(contentType))
            {
                return StringEscapeUtils.escapeJson(input);
            }
            else if (URL_ENCODED_CONTENT_TYPE.isSameMimeType(contentType))
            {
                return URLEncoder.encode(input, StandardCharsets.UTF_8);
            }
            return input;
        }

        static void write(String value, Writer writer, String message)
        {
            try
            {
                writer.append(value);
            }
            catch (IOException e)
            {
                throw new MustacheException(message + value, e);
            }
        }

        static PlbMustacheFactory getFactory(ContentType contentType)
        {
            if (ContentType.APPLICATION_JSON.isSameMimeType(contentType))
            {
                return JSON_ENCODING_FACTORY;
            }
            else if (URL_ENCODED_CONTENT_TYPE.isSameMimeType(contentType))
            {
                return URL_ENCODING_FACTORY;
            }
            return TEXT_ENCODING_FACTORY;
        }

        static class ToJsonArrayCode extends ValueCode
        {
            private static final String TOJSONARRAY = "#tojsonarray";
            private static final ObjectMapper MAPPER = new ObjectMapper();

            ToJsonArrayCode(TemplateContext tc, DefaultMustacheFactory df, String variable)
            {
                super(tc, df, variable, true);
            }

            @Override
            public Writer execute(Writer writer, List<Object> scopes)
            {
                Object resolved = get(scopes);
                if (resolved == null)
                {
                    resolved = null;
                }

                try
                {
                    JsonGenerator generator = MAPPER.createGenerator(writer);
                    if (resolved == null)
                    {
                        generator.writeNull();
                    }
                    else
                    {
                        generator.writeStartArray();

                        if (resolved instanceof Iterable)
                        {
                            for (Object o : (Iterable<?>) resolved)
                            {
                                generator.writePOJO(o);
                            }
                        }
                        else
                        {
                            generator.writePOJO(resolved);
                        }

                        generator.writeEndArray();
                    }
                    generator.flush();

                }
                catch (IOException e)
                {
                    throw new MustacheException("Failed to create jsonArray from value: " + resolved, e);
                }
                return appendText(run(writer, scopes));
            }

            /** Tries to match variable to join helper. */
            static boolean match(String variable)
            {
                return Strings.CI.indexOf(variable, TOJSONARRAY) > 0;
            }

            /** Creates a ToJsonArray code. */
            static Code create(TemplateContext tc, DefaultMustacheFactory df, String variable)
            {
                int index = Strings.CI.indexOf(variable, TOJSONARRAY);
                variable = variable.substring(0, index);
                return new ToJsonArrayCode(tc, df, variable);
            }
        }

        static class JoinCode extends ValueCode
        {
            private static final String JOIN = "#join";
            private final String delimiter;
            private final ContentType contentType;

            JoinCode(TemplateContext tc, DefaultMustacheFactory df, String variable, String delimiter, ContentType contentType)
            {
                super(tc, df, variable, true);
                this.delimiter = delimiter;
                this.contentType = contentType;
            }

            @Override
            public Writer execute(Writer writer, List<Object> scopes)
            {
                Object resolved = get(scopes);
                if (resolved == null)
                {
                    return null;
                }
                else if (resolved instanceof Iterable)
                {
                    StringJoiner joiner = new StringJoiner(encode(delimiter, contentType));
                    for (Object o : (Iterable<?>) resolved)
                    {
                        joiner.add(encode(oh.stringify(o), contentType));
                    }
                    PlbMustacheFactory.write(joiner.toString(), writer, "Failed to join value: ");
                    return appendText(run(writer, scopes));
                }
                else
                {
                    return super.execute(writer, scopes);
                }
            }

            /** Creates a join code from varible. */
            static JoinCode create(TemplateContext tc, DefaultMustacheFactory df, String variable, ContentType contentType)
            {
                int index = Strings.CI.indexOf(variable, JOIN);
                String delimiter = ",";
                // Defined delimiter after :
                if (variable.length() > (index + 5)
                        && variable.charAt(index + 5) == ':')
                {
                    delimiter = StringUtils.defaultIfBlank(variable.substring(index + 6), ",");
                }
                variable = variable.substring(0, index);
                return new JoinCode(tc, df, variable, delimiter, contentType);
            }

            /** Tries to match variable to join helper. */
            static boolean match(String variable)
            {
                return Strings.CI.indexOf(variable, JOIN) > 0;
            }
        }
    }

    /**
     * <pre>
     *  Creates a map of values from seek predicate and plain predicates.
     * To be able to properly handle values in mustache in a simple way a model
     * is created like this:
     * 
     * Assuming a seek predicate with 2 keys: 'id' and 'type'
     * 
     * {
     *   "@seekValues": [
     *     {
     *       "id": 1,
     *       "type": "typeA" 
     *     },
     *     {
     *       "id": 2,
     *       "type": "typeB"
     *     }
     *   ]
     * }
     * 
     * This corresponds to a join:
     * 
     * ON x.id = y.id
     * AND x.type = y.type
     * 
     * Assuming a plain predicate with 2 keys: 'id', 'type' where id is an IN with multiple values.
     * 
     * {
     *   "id": [1,2,3],
     *   "type": "typeA"
     * }
     * 
     * This corresponds to a where statement:
     * WHERE x.id IN (1,2,3)
     * AND x.type = 'typeA'
     * 
     * If we have a seek predicate with a single key then we transform that into a predicate since that is much
     * easier to work with in mustache so instead of:
     * 
     * {
     *   "@seekValues": [
     *     {
     *       "id": 1
     *     },
     *     {
     *       "id": 2
     *     }
     *   ]
     * }
     * 
     * we do this instead:
     * 
     * {
     *   "id": [1,2]
     * }
     * 
     * since mustache can loop values from an array this format is easier when handling JSON where we can simply
     * join the array if needed.
     * 
     * If there are both seek values and plain predicates a special field '_' is added to each seek value object
     * to be able to access the parent scoped values in case of field name conflict
     * 
     * {
     *   "id": [4],
     *   "@seekValues": [
     *     {
     *       "id": 1,
     *       "_": {
     *         "id": [4]
     *       }
     *     },
     *     {
     *       "id": 2,
     *       "_": {
     *         "id": [4]
     *       }
     *     }
     *   ]
     * }
     * 
     * This corresponds to join with a predicate.
     * 
     * ON x.id = y.id
     * AND x.id = 4
     * 
     * NOTE! This type of queries are very seldom used and also they should be optimized away in PLB core framework
     * which they are not at the time of writing.
     * </pre>
     */
    static Map<String, Object> createValues(IExecutionContext context, ISeekPredicate seekPredicate, List<Predicate> predicates, boolean describe)
    {
        Map<String, Object> result = new HashMap<>();

        // First construct the predicates on root level
        if (!predicates.isEmpty())
        {
            for (Predicate predicate : predicates)
            {
                List<Object> values = predicate.values()
                        .stream()
                        .map(e -> e.eval(context)
                                .valueAsObject(0))
                        .toList();
                result.put(predicate.name(), predicate.single() ? values.get(0)
                        : values);
            }
        }

        if (seekPredicate != null)
        {
            List<ISeekKey> seekKeys;
            if (describe)
            {
                // Construct fake values in describe mode since we cannot get any without
                seekKeys = seekPredicate.getIndexColumns()
                        .stream()
                        .map(c -> (ISeekKey) new ISeekKey()
                        {
                            @Override
                            public ValueVector getValue()
                            {
                                return ValueVector.literalAny(1, "<index values>");
                            }
                        })
                        .toList();
            }
            else
            {
                seekKeys = seekPredicate.getSeekKeys(context);
            }

            if (seekKeys.size() == 1)
            {
                result.compute(seekPredicate.getIndexColumns()
                        .get(0), (k, v) ->
                        {
                            if (v == null)
                            {
                                return new ValueVectorList(seekKeys.get(0)
                                        .getValue());
                            }

                            // If there are conflicting keys in predicate and seekValues
                            // merge those into one list
                            List<Object> l = seekKeys.get(0)
                                    .getValue()
                                    .toList();

                            List<Object> r = new ArrayList<>(l.size() + 1);
                            if (v instanceof List<?> ll)
                            {
                                r.addAll(ll);
                            }
                            else
                            {
                                r.add(v);
                            }
                            r.addAll(l);
                            return r;
                        });
            }
            else
            {
                // Make a copy of the predicates to be able to access the predicates inside the seek values array
                // with '$' notion
                Map<String, Object> parentScope = new HashMap<>(result);

                int rowCount = seekKeys.get(0)
                        .getValue()
                        .size();
                int columnCount = seekPredicate.getIndexColumns()
                        .size();
                List<Map<String, Object>> seekValues = new ArrayList<>(seekKeys.get(0)
                        .getValue()
                        .size());

                for (int i = 0; i < rowCount; i++)
                {
                    Map<String, Object> row = new HashMap<>(columnCount + 1, 1.0f);
                    if (!parentScope.isEmpty())
                    {
                        row.put("_", parentScope);
                    }
                    seekValues.add(row);
                    for (int j = 0; j < columnCount; j++)
                    {
                        row.put(seekPredicate.getIndexColumns()
                                .get(j),
                                seekKeys.get(j)
                                        .getValue()
                                        .valueAsObject(i));
                    }
                }
                result.put(SEEK_VALUES, seekValues);
            }
        }

        return result;
    }

    /** Wrapper that exposes a {@link ValueVector} as a {@link List}. */
    static class ValueVectorList extends AbstractList<Object>
    {
        private final ValueVector vector;

        ValueVectorList(ValueVector vector)
        {
            this.vector = vector;
        }

        @Override
        public int size()
        {
            return vector.size();
        }

        @Override
        public Object get(int index)
        {
            return vector.valueAsObject(index);
        }
    }

    /** Compiles and processes pattern with provided values. */
    static String replacePattern(String bodyPattern, ContentType contentType, Map<String, Object> values)
    {
        Mustache mustache = compile(bodyPattern, contentType);
        StringWriter sw = new StringWriter();
        mustache.execute(sw, values);
        return sw.toString();
    }

    /** Extracts fields from body pattern. */
    static List<String> extractFields(String bodyPattern)
    {
        Mustache mustache = compile(bodyPattern, ContentType.TEXT_PLAIN);
        Set<String> fields = new HashSet<>();
        walk(mustache, fields);
        return fields.stream()
                .toList();
    }

    private static Mustache compile(String bodyPattern, ContentType contentType)
    {
        PlbMustacheFactory mf = PlbMustacheFactory.getFactory(contentType);
        Mustache mustache = mf.compile(new StringReader(bodyPattern), "body");
        return mustache;
    }

    private static void walk(Code test, Set<String> fields)
    {
        Consumer<String> adder = s ->
        {
            String field = s;
            if (field.startsWith(SEEK_VALUES))
            {
                return;
            }
            // Parent scope access
            else if (field.startsWith("_"))
            {
                field = field.substring(1);
            }
            if (field.charAt(0) == '.')
            {
                field = field.substring(1);
            }

            // Everything with a dot-something like 'list.size' should be
            // truncated to 'list'
            int index = field.indexOf('.');
            if (index > 0)
            {
                field = field.substring(0, index);
            }

            if (isBlank(field))
            {
                return;
            }

            fields.add(field);
        };

        for (Code code : test.getCodes())
        {
            if (code instanceof ValueCode)
            {
                adder.accept(code.getName());
            }
            else if (code instanceof NotIterableCode)
            {
                adder.accept(code.getName());
                walk(code, fields);
            }
            else if (code instanceof IterableCode)
            {
                adder.accept(code.getName());
                walk(code, fields);
            }
        }
    }
}
