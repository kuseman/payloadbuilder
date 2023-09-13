package se.kuseman.payloadbuilder.core.statement;

import static java.util.Collections.emptyList;

import java.util.List;

/** Base class for statements */
// CSOFF
public abstract class Statement
// CSON
{
    /** Accept method for visitor */
    public abstract <TR, TC> TR accept(StatementVisitor<TR, TC> visitor, TC context);

    /** Return child statements of this node */
    public List<Statement> getChildren()
    {
        return emptyList();
    }
}
