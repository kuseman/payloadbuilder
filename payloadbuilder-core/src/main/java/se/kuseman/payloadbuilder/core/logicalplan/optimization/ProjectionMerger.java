package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.expression.IColumnExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.HasAlias;
import se.kuseman.payloadbuilder.core.expression.HasAlias.Alias;

/** Class that merges two projects that is nested. */
public class ProjectionMerger
{
    private ProjectionMerger()
    {
    }

    /**
     * Merges outer expressions with inner. Indexes the projection names from inner and replaces all occurrences in outer
     */
    public static List<IExpression> replace(List<IExpression> outerExpressions, List<IExpression> innerExpressions)
    {
        Map<String, IExpression> innerByName = new HashMap<>(innerExpressions.size());

        for (IExpression e : innerExpressions)
        {
            if (e instanceof HasAlias ha)
            {
                String alias = ha.getAlias()
                        .getAlias()
                        .toLowerCase();
                // Unwrap all alias expressions since we don't need those when replacing the outer
                if (e instanceof AliasExpression ae)
                {
                    e = ae.getExpression();
                }

                innerByName.put(alias, e);
            }
        }
        ExpressionReplacer.Context context = new ExpressionReplacer.Context();
        context.inner = innerExpressions;
        context.innerByName = innerByName;

        List<IExpression> result = new ArrayList<>(outerExpressions.size());
        for (IExpression outer : outerExpressions)
        {
            String alias = "";
            String outputAlias = "";
            boolean internal = false;
            if (outer instanceof HasAlias ha)
            {
                Alias a = ha.getAlias();
                alias = a.getAlias();
                outputAlias = a.getOutputAlias();

                if (outer instanceof AliasExpression ae)
                {
                    internal = ae.isInternal();
                }
            }
            else
            {
                outputAlias = outer.toString();
            }

            IExpression outerResult = ExpressionReplacer.INSTANCE.visit(outer, context);

            // We know that the result cannot be an alias expression since we have unwrapped all of those
            // before replacing
            String resultAlias = "";
            // String resultOutputAlias = "";
            if (outerResult instanceof HasAlias ha)
            {
                Alias a = ha.getAlias();
                resultAlias = a.getAlias();
            }

            // Make sure we have the same alias as the original expression since that could be changed now when we replaced
            if (!alias.equalsIgnoreCase(resultAlias))
            {
                result.add(new AliasExpression(outerResult, alias, outputAlias, internal));
            }
            else
            {
                result.add(outerResult);
            }
        }

        return result;
    }

    /** Expression rewriter that replaces column expressions referenced in inner projection */
    private static class ExpressionReplacer extends ARewriteExpressionVisitor<ExpressionReplacer.Context>
    {
        private static final ExpressionReplacer INSTANCE = new ExpressionReplacer();

        static class Context
        {
            List<IExpression> inner;
            Map<String, IExpression> innerByName;
        }

        @Override
        public IExpression visit(IColumnExpression expression, Context context)
        {
            int ordinal = expression.getOrdinal();
            String column = expression.getColumn();

            if (column != null)
            {
                // Return replacement or it self if no replacement was found
                return context.innerByName.getOrDefault(column.toLowerCase(), expression);
            }
            else if (ordinal >= 0)
            {
                // Replace by ordinal
                IExpression e = context.inner.get(ordinal);
                // Unwrap alias expression if any, not needed
                if (e instanceof AliasExpression ae)
                {
                    return ae.getExpression();
                }
                return e;
            }

            return expression;
        }
    }
}
