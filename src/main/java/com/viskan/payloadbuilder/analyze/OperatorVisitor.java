package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.operator.CachingOperator;
import com.viskan.payloadbuilder.operator.NestedLoop;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.parser.tree.AJoin;
import com.viskan.payloadbuilder.parser.tree.ATreeVisitor;
import com.viskan.payloadbuilder.parser.tree.Join;
import com.viskan.payloadbuilder.parser.tree.PopulateTableSource;
import com.viskan.payloadbuilder.parser.tree.Query;
import com.viskan.payloadbuilder.parser.tree.Table;
import com.viskan.payloadbuilder.parser.tree.TableSource;
import com.viskan.payloadbuilder.parser.tree.TableSourceJoined;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.Iterator;
import java.util.Random;
import java.util.stream.IntStream;

/** Operator that creates an operator from a query */
public class OperatorVisitor extends ATreeVisitor<Operator, OperatorVisitor.Context>
{
    private static final OperatorVisitor VISITOR = new OperatorVisitor();
    
    static class Context
    {
        CatalogRegistry catalogRegistry;
        TableAlias alias;

        Operator outer;
        int count;
    }
    
    /** Create operator */
    public static Operator create(CatalogRegistry catalogRegistry, Query query)
    {
        Context context = new Context();
        TableAlias alias = TableAliasBuilder.build(query);
        context.catalogRegistry = catalogRegistry;
        context.alias = alias;
        return query.accept(VISITOR, context);
    }
    
    @Override
    public Operator visit(Query query, Context context)
    {
        Operator operator = visit(query.getFrom(), context);
        
        return operator;
    }
    
    /*
     * 
op0 = spool("parents", scan(source))

op1 = spool("parents", join(op0, scanWithSpool("parents", article)))

-- (aa)

op2 = scanWithSpool("parents", articleAttribute)
op3 = spool("parents", join(op2, scanWithSpool("parents", articlePrice)))

op4 = join(op3, scanWithSpool("parents", attribute1))
    
-- (/aa)

op5 = join(op1, op4)

-------

op0 = scan(source)

op1 = scan(article)
op2 = join(op0, op1)

-- (aa)

op3 = scan(articleAttribute)
op4 = scan(aticlePrice)
op5 = join(op3, op4)

-- (/aa)

op6 = join(op2, op5)



     */
    
    @Override
    public Operator visit(TableSourceJoined joinedTableSource, Context context)
    {
        // TODO: does next TableSource need parents, then put spool infront
        
//        if (context.outer == null)
//        {
//            System.out.print("op0 = ");
//        }
//        
//        System.out.print("spool(\"parents\", ");
        context.outer = joinedTableSource.getTableSource().accept(this, context);
//        System.out.println(")");
        
        // TODO: detect join order, if possible
        //       detect join dependency (or do it in TableAliasBuilder)
        
        for (AJoin join : joinedTableSource.getJoins())
        {
//            System.out.print("spool(\"parents\", join(op" + (context.count - 1) + ", ");
            context.outer = join.accept(this, context);
//            System.out.println(")");
        }
        
        return context.outer;
    }
    
    @Override
    public Operator visit(Join join, Context context)
    {
        // TODO: detect physical join operator (hashjoin, merge join)
        //       push down of predicate to table source
        //       does inner need outer rows in a RowSpool?
        //       wrap outer with a row spool and inner with a spool scan
        
        Operator outer = context.outer;
        TableSource tableSource = join.getTableSource();
        
        int outerCount = context.count - 1;
        Operator inner = tableSource.accept(this, context);
        int innerCount = context.count - 1;
        
        if (!tableSource.isPopulating())
        {
            System.out.println("Clear parent rows!");
        }

        System.out.println("op" + (context.count++) + " = join(op" + outerCount + ", op" + innerCount + ")");
            
//        System.out.println("Creating join " + tableSource);
        return new NestedLoop(outer, new CachingOperator(inner), row -> true, tableSource.isPopulating());
    }
    
    @Override
    public Operator visit(PopulateTableSource populatingJoin, Context context)
    {
        TableAlias parent = context.alias;
        context.alias = context.alias.getChildAlias(populatingJoin.getAlias());
//        if (hasChildJoins)
//        {
//            System.out.println("Stream parents child rows for: " + alias);
//        }
        
        // TODO: wrap table source with parents child rows iterator
        Operator result = populatingJoin.getTableSourceJoined().accept(this, context);

        // Restore prev parent
        context.alias = parent;
        
//        if (hasChildJoins)
//        {
//            TableAlias alias = context.alias.getChildAlias(populatingJoin.getAlias());
//            context.outer = c -> IteratorUtils.getChildRowsIterator(result.open(c), alias.getParentIndex()).iterator();
//        }
        
        

        
        // TODO: wrap table source with order by etc.
        // TODO: wrap result in parents row streamer
        
        return result;
    }
    
    @Override
    public Operator visit(Table table, Context context)
    {
        final TableAlias alias = getAlias(context.alias, table.getAlias());
        System.out.println("op" + (context.count++) + " = scan(" + table.getTable() + "," + alias + ")");
        // TODO: create operator from catalog/provider
        return new Operator()
        {
            
            @Override
            public Iterator<Row> open(OperatorContext c)
            {
                int to = 10;
//                if (alias.getAlias().equals("ap"))
//                {
//                    to= 3;
//                }
                return IntStream.range(0, new Random().nextInt(to))
                        .mapToObj(i -> Row.of(alias, i, new Object[] { i})).iterator();
            }
            
            @Override
            public String toString()
            {
                return table.getTable().toString();
            }
        };
    }
    
    private TableAlias getAlias(TableAlias parent, String alias)
    {
        if (isBlank(alias) || parent.getAlias().equals(alias))
        {
            return parent;
        }
        return parent.getChildAlias(alias);
    }
}
