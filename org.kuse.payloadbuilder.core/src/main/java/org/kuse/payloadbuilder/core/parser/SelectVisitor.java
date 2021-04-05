package org.kuse.payloadbuilder.core.parser;

/**
 * Visitor definition of tree
 *
 * @param <TR> Return type
 * @param <TC> Context type
 */
public interface SelectVisitor<TR, TC>
{
    //CSOFF
    TR visit(Select select, TC context);

    TR visit(TableSourceJoined joinedTableSource, TC context);

    TR visit(SortItem sortItem, TC context);

    TR visit(ExpressionSelectItem expressionSelectItem, TC context);

    TR visit(NestedSelectItem nestedSelectItem, TC context);

    TR visit(AsteriskSelectItem selectItem, TC context);

    TR visit(Table table, TC context);

    TR visit(TableFunction tableFunction, TC context);

    TR visit(Join join, TC context);

    TR visit(Apply apply, TC context);

    TR visit(SubQueryTableSource populatingJoin, TC context);
    //CSON
}
