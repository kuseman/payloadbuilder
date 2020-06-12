package com.viskan.payloadbuilder.evaluation;

import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.QualifiedReferenceExpression.QualifiedReferenceContainer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

/** Context used during evaluation */
public class EvaluationContext
{
    private final long now = System.currentTimeMillis();
    private List<Object> lambdaValues;
    private List<QualifiedReferenceContainer> qualifiedContainers;
    private Map<String, Object> parameters = new HashMap<>();

    public long getNow()
    {
        return now;
    }
    
    /** Get container for provided qname and unique id */
    public QualifiedReferenceContainer getContainer(QualifiedName qname, int containerId)
    {
        return getOrCreateItem(
                qualifiedContainers,
                l -> qualifiedContainers = l,
                () -> new QualifiedReferenceContainer(qname),
                containerId);
    }

    /** Get lambda value in scope for provided id */
    public Object getLambdaValue(int lambdaId)
    {
        return getOrCreateItem(
                lambdaValues,
                l -> lambdaValues = l,
                null,
                lambdaId);
    }

    /** Set lambda value in scope for provided id */
    public void setLambdaValue(int lambdaId, Object value)
    {
        getOrCreateItem(
                lambdaValues,
                l -> lambdaValues = l,
                null,
                lambdaId);
        lambdaValues.set(lambdaId, value);
    }

    /** Clear state between evalautions */
    public void clear()
    {
        if (lambdaValues != null)
        {
            lambdaValues.clear();
        }
        if (qualifiedContainers != null)
        {
            qualifiedContainers.forEach(c -> c.clear());
        }
    }

    private <T> T getOrCreateItem(List<T> list, Consumer<List<T>> setter, Supplier<T> newItem, int itemIndex)
    {
        if (list == null || list.size() < itemIndex)
        {
            if (list == null)
            {
                list = new ArrayList<>();
                setter.accept(list);
            }
        }

        int diff = 1 + (itemIndex - list.size());
        if (diff > 0)
        {
            list.addAll(Collections.nCopies(diff, null));
        }

        T item = list.get(itemIndex);
        if (item == null && newItem != null)
        {
            item = newItem.get();
            list.set(itemIndex, item);
        }
        return item;
    }

    /** Return value for named parameter */
    public Object getParameterValue(String name)
    {
        return parameters.get(name);
    }

    /** Set parameters */
    public void setParameters(Map<String, Object> params)
    {
        parameters = params;
    }
}
