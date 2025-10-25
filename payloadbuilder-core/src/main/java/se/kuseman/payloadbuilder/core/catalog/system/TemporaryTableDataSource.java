package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.Objects;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.TemporaryTable;

/** Datasource of temporary tables. */
class TemporaryTableDataSource implements IDatasource
{
    private final QualifiedName name;
    private final ISeekPredicate seekPredicate;

    TemporaryTableDataSource(QualifiedName name, ISeekPredicate seekPredicate)
    {
        this.name = name;
        this.seekPredicate = seekPredicate;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        TemporaryTable temporaryTable = ((ExecutionContext) context).getSession()
                .getTemporaryTable(name);
        if (seekPredicate != null)
        {
            return temporaryTable.getIndexIterator(context, seekPredicate);
        }

        // Return the tuple vector from context with the planned schema
        return TupleIterator.singleton(temporaryTable.getTupleVector());
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null)
        {
            return false;
        }
        else if (obj instanceof TemporaryTableDataSource that)
        {
            return name.equals(that.name)
                    && Objects.equals(seekPredicate, that.seekPredicate);
        }
        return false;
    }
}
