package com.viskan.payloadbuilder;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.IOException;
import java.util.Collection;

import org.apache.commons.lang3.ArrayUtils;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

@JsonSerialize(using = Result.ResultSerializer.class)
public class Result
{
    public static final Object EMPTY = new Object();
    private final String[] columns;
    private final Object[] values;

    public Result(String[] columns, Object[] values)
    {
        this.columns = requireNonNull(columns);
        this.values = requireNonNull(values);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(ArrayUtils.toString(columns));

        if (values != null)
        {
            sb.append(" {");
            for (Object v : values)
            {
                if (v == null)
                {
                    sb.append("null");
                }
                else if (v instanceof Iterable)
                {
                    sb.append("[");
                    for (Object o : (Iterable) v)
                    {
                        sb.append(ArrayUtils.toString(o));
                        sb.append(", ");
                    }
                    sb.append("]");
                    continue;
                }
                else
                {
                    sb.append(v);
                }
                sb.append(" ");
            }
            sb.append("}");
        }
        ;

        return sb.toString();
    }
    
    /** Jackson serializer for result */
    public static class ResultSerializer extends JsonSerializer<Result>
    {
        /** Serialize columns/values pair */
        @SuppressWarnings("unchecked")
        public static void serialize(
                JsonGenerator gen,
                SerializerProvider provider,
                String[] columnNames,
                Object[] values) throws IOException, JsonProcessingException
        {
            gen.writeStartObject();
            if (columnNames == null)
            {
                gen.writeEndObject();
                return;
            }

            int size = columnNames.length;
            for (int i = 0; i < size; i++)
            {
                String fieldName = columnNames[i];
                if (isBlank(fieldName))
                {
                    continue;
                }

                Object data = i < values.length ? values[i] : null;
                if (data == EMPTY)
                {
                    continue;
                }

//                data = unwrap(data);

                // Anonymous iterable, create array
                if (data instanceof Iterable && !(data instanceof Collection))
                {
                    try
                    {
                        gen.writeArrayFieldStart(fieldName);

                        Iterable<Object> iter = (Iterable<Object>) data;
                        for (Object obj : iter)
                        {
                            provider.defaultSerializeValue(obj, gen);
                        }
                        gen.writeEndArray();
                    }
                    catch (Exception e)
                    {
//                        LOGGER.error("Error serializing result.", e);
                        gen.writeEndArray();
                    }
                }
//                else if (isAnonymous(data))
//                {
//                    gen.writeStringField(fieldName, data.toString());
//                }
                else
                {
                    gen.writeFieldName(fieldName);
                    try
                    {
                        provider.defaultSerializeValue(data, gen);
                    }
                    catch (JsonMappingException e)
                    {
//                        LOGGER.error("Error serializing result.", e);
                        gen.writeNull();
                    }
                }
            }
            gen.writeEndObject();
        }

        @Override
        public void serialize(Result value, JsonGenerator gen, SerializerProvider provider) throws IOException, JsonProcessingException
        {
            serialize(gen, provider, value.columns, value.values);
        }
    }
    
}
