package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.ISeekKey;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.SelectedValueVector;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.execution.StatementContext;

/** Seek operator that seeks a datasource with a set of seek keys */
public class IndexSeek extends TableScan
{
    private final ISeekPredicate seekPredicate;

    public IndexSeek(int nodeId, Schema schema, TableSourceReference tableSource, String catalogAlias, boolean tempTable, ISeekPredicate seekPredicate, IDatasource datasource, List<Option> options)
    {
        super(nodeId, schema, tableSource, catalogAlias, tempTable, datasource, options);
        this.seekPredicate = requireNonNull(seekPredicate, "seekPredicate");
    }

    @Override
    public String getName()
    {
        return "Index Seek: " + (tempTable ? "#"
                : "")
               + seekPredicate.getIndex();
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> properties = new LinkedHashMap<>(super.getDescribeProperties(context));
        properties.put(IDatasource.INDEX, seekPredicate.getIndexColumns());
        properties.put("Seek Keys", seekPredicate.toString());

        int batchSize = getBatchSize(context);
        if (batchSize > 0)
        {
            properties.put("Batch Size", batchSize);
        }
        return properties;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        List<ISeekKey> seekKeys = seekPredicate.getSeekKeys(context);
        if (seekKeys.isEmpty())
        {
            return TupleIterator.EMPTY;
        }

        int batchSize = getBatchSize(context);
        int seekKeySize = seekKeys.get(0)
                .getValue()
                .size();
        if (batchSize < 0
                || seekKeySize <= batchSize)
        {
            return super.execute(context);
        }

        StatementContext statementContext = (StatementContext) context.getStatementContext();
        DatasourceOptions dataSourceOptions = new DatasourceOptions(options);

        return new TupleIterator()
        {
            int currentRow = 0;
            TupleIterator currentIterator;
            TupleVector next;

            @Override
            public int estimatedBatchCount()
            {
                return (int) Math.ceil(seekKeySize / (float) batchSize);
            }

            @Override
            public int estimatedRowCount()
            {
                // NOTE! We will at least have seek key size, but mostly likely more
                return seekKeySize;
            }

            @Override
            public void close()
            {
                // Put back original keys
                statementContext.setIndexSeekKeys(seekKeys);
            }

            @Override
            public TupleVector next()
            {
                if (next == null)
                {
                    throw new NoSuchElementException();
                }
                TupleVector r = next;
                next = null;
                return r;
            }

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    // Continue batching
                    if (currentIterator == null)
                    {
                        // We're done
                        if (currentRow >= seekKeySize)
                        {
                            return false;
                        }

                        int start = currentRow;
                        int end = currentRow + batchSize;
                        if (end > seekKeySize)
                        {
                            end = seekKeySize;
                        }
                        currentRow = end;

                        // Construct batch seek keys
                        int size = seekKeys.size();
                        List<ISeekKey> batchSeekKeys = new ArrayList<>(size);
                        ValueVector selection = ValueVector.range(start, end);
                        for (int i = 0; i < size; i++)
                        {
                            ISeekKey seekKey = seekKeys.get(i);
                            ValueVector batchVector = SelectedValueVector.select(seekKey.getValue(), selection);
                            batchSeekKeys.add(() -> batchVector);
                        }
                        statementContext.setIndexSeekKeys(batchSeekKeys);
                        currentIterator = datasource.execute(context, dataSourceOptions);
                        continue;
                    }
                    else if (!currentIterator.hasNext())
                    {
                        currentIterator.close();
                        currentIterator = null;
                        continue;
                    }

                    // Concat the data source up to batch size, this might happen if catalog don't implement batch size correct
                    TupleVector vector = PlanUtils.concat(context, currentIterator, batchSize);
                    Schema vectorSchema = vector.getSchema();
                    validate(context, vectorSchema, vector.getRowCount());
                    if (!asteriskSchema)
                    {
                        next = vector;
                    }
                    else
                    {
                        // Attach table source to all asterisk columns in the vector to make column evaluation work properly
                        final Schema schema = recreateSchema(tableSource, vectorSchema);
                        next = new TupleVector()
                        {
                            @Override
                            public Schema getSchema()
                            {
                                return schema;
                            }

                            @Override
                            public int getRowCount()
                            {
                                return vector.getRowCount();
                            }

                            @Override
                            public ValueVector getColumn(int column)
                            {
                                return vector.getColumn(column);
                            }
                        };
                    }
                }
                return true;
            }
        };
    }

    private int getBatchSize(IExecutionContext context)
    {
        for (Option option : options)
        {
            if (DatasourceOptions.BATCH_SIZE.equals(option.getOption()))
            {
                ValueVector vv = option.getValueExpression()
                        .eval(context);
                return !vv.isNull(0) ? vv.getInt(0)
                        : -1;
            }
        }
        return -1;
    }

    @Override
    public int hashCode()
    {
        return nodeId;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof IndexSeek that)
        {
            return super.equals(obj)
                    && seekPredicate.equals(that.seekPredicate);
        }
        return false;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("Index Seek (").append(nodeId)
                .append("): ");
        sb.append(tableSource.toString());
        sb.append(" (keys: ")
                .append(seekPredicate)
                .append(")");
        return sb.toString();
    }
}
