package se.kuseman.payloadbuilder.core.test;

import static java.util.Collections.emptyList;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;

/** Harness table */
public class TestTable
{
    private String name;
    private List<String> columns = emptyList();
    @JsonDeserialize(
            using = ResolvedTypeDeserializer.class)
    private List<ResolvedType> types = emptyList();
    private List<Object[]> rows;

    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    public List<String> getColumns()
    {
        return columns;
    }

    public void setColumns(List<String> columns)
    {
        this.columns = columns;
    }

    public List<ResolvedType> getTypes()
    {
        return types;
    }

    public void setTypes(List<ResolvedType> types)
    {
        this.types = types;
    }

    public List<Object[]> getRows()
    {
        return rows;
    }

    public void setRows(List<Object[]> rows)
    {
        this.rows = rows;
    }

    /** Deserializer for {@link ResolvedType}. */
    static class ResolvedTypeDeserializer extends JsonDeserializer<List<ResolvedType>>
    {
        @Override
        public List<ResolvedType> deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException
        {
            List<Map<String, Object>> list = p.readValueAs(new TypeReference<List<Map<String, Object>>>()
            {
            });

            return list.stream()
                    .map(ResolvedTypeDeserializer::from)
                    .toList();
        }

        private static ResolvedType from(Map<String, Object> map)
        {
            Column.Type type = Column.Type.valueOf(String.valueOf(map.get("type")));

            @SuppressWarnings("unchecked")
            List<Map<String, Object>> schemaList = (List<Map<String, Object>>) map.get("schema");
            Schema schema = from(schemaList);

            return new ResolvedType(type, null, schema);
        }

        @SuppressWarnings("unchecked")
        private static Schema from(List<Map<String, Object>> list)
        {
            if (list == null)
            {
                return null;
            }

            return new Schema(list.stream()
                    .map(m -> Column.of(String.valueOf(m.get("name")), from((Map<String, Object>) m.get("type"))))
                    .toList());
        }
    }
}
