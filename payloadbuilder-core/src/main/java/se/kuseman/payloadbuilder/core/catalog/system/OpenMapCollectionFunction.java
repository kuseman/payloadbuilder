package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/**
 * Table value function that extracts row from a collection of maps in target expression
 *
 * <pre>
 *
 * Example
 *
 * SELECT field
 * FROM source s
 * OUTER APPLY
 * (
 *   open_map_collection(s.article_attribute.attribute1.buckets) a1Buckets
 *   INNER JOIN attribute1 a1
 *     ON a1.attr1_id == a1Buckets.key
 *   ORDER BY a1.attr1_code
 * ) attribute1
 * </pre>
 */
class OpenMapCollectionFunction extends TableFunctionInfo
{
    OpenMapCollectionFunction(Catalog catalog)
    {
        super(catalog, "open_map_collection");
    }

    @Override
    public String getDescription()
    {
        return "Table valued function that opens a row set from a collection of maps." + System.lineSeparator()
               + System.lineSeparator()
               + "Ex. "
               + System.lineSeparator()
               + "set @rows = '[ { \"key\": 123 }, { \"key\": 456 } ]'"
               + System.lineSeparator()
               + "select * from "
               + getName()
               + "(json_value(@rows)) "
               + System.lineSeparator()
               + System.lineSeparator()
               + "Will yield a row set like: "
               + System.lineSeparator()
               + System.lineSeparator()
               + "key"
               + System.lineSeparator()
               + "---"
               + System.lineSeparator()
               + "123"
               + System.lineSeparator()
               + "456";
    }

    @Override
    public int arity()
    {
        return 1;
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments, IDatasourceOptions options)
    {
        ValueVector vector = eval(context, arguments.get(0));

        @SuppressWarnings("unchecked")
        Collection<Map<String, Object>> collection = (Collection<Map<String, Object>>) vector.getValue(0);

        if (collection == null)
        {
            return TupleIterator.EMPTY;
        }

        final List<Map<String, Object>> list = (collection instanceof List) ? (List<Map<String, Object>>) collection
                : new ArrayList<>(collection);

        Set<String> columns = new LinkedHashSet<>();

        for (Map<String, Object> map : list)
        {
            columns.addAll(map.keySet());
        }

        final Schema schema = new Schema(columns.stream()
                .map(c -> Column.of(c, ResolvedType.of(Type.Any)))
                .collect(toList()));
        final int rowCount = list.size();

        return TupleIterator.singleton(new TupleVector()
        {
            @Override
            public Schema getSchema()
            {
                return schema;
            }

            @Override
            public int getRowCount()
            {
                return rowCount;
            }

            @Override
            public ValueVector getColumn(int column)
            {
                final String name = schema.getColumns()
                        .get(column)
                        .getName();

                return new ValueVector()
                {
                    @Override
                    public ResolvedType type()
                    {
                        return ResolvedType.of(Type.Any);
                    }

                    @Override
                    public int size()
                    {
                        return rowCount;
                    }

                    @Override
                    public boolean isNull(int row)
                    {
                        return getValue(row) == null;
                    }

                    @Override
                    public Object getValue(int row)
                    {
                        return list.get(row)
                                .get(name);
                    }
                };
            }
        });
    }
}
