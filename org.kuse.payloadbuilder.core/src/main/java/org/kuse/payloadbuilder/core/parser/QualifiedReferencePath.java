package org.kuse.payloadbuilder.core.parser;

import org.kuse.payloadbuilder.core.operator.TableAlias;

/** Traversal path for a {@link QualifiedReferenceExpression}. */
class QualifiedReferencePath
{
    /**
     * <pre>
     * If multiple TableAliases having this same path then
     * a chain of {@link #QualifiedReferencePath()} is created.
     *
     * Ie. If concat is made from two different row sets from two different
     * tables and then passed to a map function accessing the same column name
     * then it's practically two different paths from the same {@link QualifiedReferenceExpression}
     * </pre>
     */
    //    private final QualifiedReferencePath parent;

    /** Which alias this path is calculated from */
    //    private final TableAlias tableAlias;
    /** Number of parent steps from {@link #tableAlias} */
    //    private final int parentSteps;
    /** Index of child alias after {@link #parentSteps} traversal */
    //    private final int childAliasIndex;
    /** If this path is a column pointer then this is the index */
    //    private final int columnIndex;

    /**
     * <pre>
     * Parts that's left after lookup. Object lookup after destination. Ie. a Map-traversal.
     */
    //    private final List<String> parts;

    /**
     * Calculates lookup path. From: table alias To: qname
     **/
    @SuppressWarnings("unused")
    static QualifiedReferencePath calculatePath(TableAlias tableAlias, QualifiedName qname)
    {
        //        TableAlias current = tableAlias;
        //        int partIndex = 0;
        //        Row resultRow = row;
        //
        //        List<String> parts = qname.getParts();
        //        int parentSteps = 0;
        //        int childAliasIndex = -1;

        // aa.obj
        //

        /*
         * 1: (val)
         *   child-alias
         *   parent-collection
         *   column
         * 2: (a.val)
         *   child-alias + column
         *   parent-collection + column
         *   parent-collection + child-alias
         *   column + left over part
         *   left over parts
         * 3: (a.c.val)
         *   child-alias
         *   parent-collection
         */

        //        while (partIndex < parts.size() - 1)
        //        {
        //            String part = parts.get(partIndex);
        //
        //            // 1. Alias match, move on
        //            if (Objects.equals(part, current.getAlias()))
        //            {
        //                partIndex ++;
        //                continue;
        //            }
        //
        //            // 2. Child alias
        //            TableAlias alias = current.getChildAlias(part);
        //            if (alias != null)
        //            {
        //                partIndex ++;
        //                current = alias;
        //                childAliasIndex = current.getParentIndex();
        ////                List<Row> childAlias = resultRow.getChildRows(alias.getParentIndex());
        ////                resultRow = !childAlias.isEmpty() ? (Row) CollectionUtils.get(childAlias, 0) : null;
        //                continue;
        //            }
        //
        //            if (current.getParent() == null)
        //            {
        //                break;
        //            }
        //
        //            // TODO: access parent collection
        //
        //
        //            // 3. Parent alias match upwards
        ////            resultRow = resultRow.getParent();
        //            current = current.getParent();// resultRow != null ? resultRow.getTableAlias() : null;
        //            parentSteps ++;
        //        }
        //
        //        if (resultRow == null)
        //        {
        //            return null;
        //        }

        return null;
    }
}
