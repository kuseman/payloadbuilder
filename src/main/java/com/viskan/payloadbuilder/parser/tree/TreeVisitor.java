package com.viskan.payloadbuilder.parser.tree;

/** Visitor definition of tree */
public interface TreeVisitor<TR, TC>
{
    TR visit(Query query, TC context);
    TR visit(TableSourceJoined joinedTableSource, TC context);
    TR visit(SortItem sortItem, TC context);
    TR visit(ExpressionSelectItem expressionSelectItem, TC context);
    TR visit(NestedSelectItem nestedSelectItem, TC context);
    TR visit(Table table, TC context);
    TR visit(TableFunction tableFunction, TC context);
    TR visit(Join join, TC context);
    TR visit(Apply apply, TC context);
    TR visit(PopulateTableSource populatingJoin, TC context);
}
