package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/** Operator that spools rows before proceeding with child operator */
public class RowSpool implements Operator
{
    private final String key;
    private final Operator operator;
    private final Operator downstream;

    public RowSpool(String key, Operator operator, Operator downstream)
    {
        this.key = requireNonNull(key, "key");
        this.operator = requireNonNull(operator, "operator");
        this.downstream = requireNonNull(downstream, "downstream");
    }
    
    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        Iterator<Row> it = operator.open(context);
        List<Row> rows = new ArrayList<>();
        while (it.hasNext())
        {
            rows.add(it.next());
        }
        context.storeSpoolRows(key, rows);
        return downstream.open(context);
    }
    
    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return "SPOOL (key=" + key + ") " + System.lineSeparator()  +
                indentString + operator.toString(indent + 1) + System.lineSeparator() + 
                indentString + downstream.toString(indent + 1); 
    }
}
