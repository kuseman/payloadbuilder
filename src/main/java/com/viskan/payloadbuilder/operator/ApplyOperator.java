package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;

/** OPerator that handles apply operations */
public class ApplyOperator implements Operator
{
    private final Operator outer;
    private final Operator inner;
    private final boolean populating;
    
    public ApplyOperator(Operator outer, Operator inner, boolean populating)
    {
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "inner");
        this.populating = populating;
    }
    
    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        final Iterator<Row> it = outer.open(context);
        return new Iterator<Row>()
        {
            Row next;
            Row currentOuter;
            Iterator<Row> ii;
            boolean hit;

            @Override
            public Row next()
            {
                Row r = next;
                next = null;
                return r;
            }

            @Override
            public boolean hasNext()
            {
                return next != null || setNext();
            }

            boolean setNext()
            {
                while (next == null)
                {
                    if (ii == null && !it.hasNext())
                    {
                        return false;
                    }

                    if (currentOuter == null)
                    {
                        currentOuter = it.next();
                        hit = false;
                    }

                    if (ii == null)
                    {
                        ii = inner.open(context);
                    }

                    if (!ii.hasNext())
                    {
                        ii = null;
                        if (populating && hit)
                        {
                            next = currentOuter;
                        }
                            
                        currentOuter = null;
                        continue;
                    }
                    
                    Row prevOuter = context.getCurrentOuter();
                    context.setCurrentOuter(currentOuter);
                    
//                    context.set
                    

//                    if (currentInner.evaluatePredicate(currentOuter, predicate))
//                    {
//                        next = rowMerger.merge(currentOuter, currentInner, populating);
//                        if (populating)
//                        {
//                            next = null;
//                        }
//                        hit = true;
//                    }
                }

                return next != null;
            }
        };
    }

}
