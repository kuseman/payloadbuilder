package com.viskan.payloadbuilder.codegen;

import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.Row;
import com.viskan.payloadbuilder.parser.EvaluationContext;
import com.viskan.payloadbuilder.parser.Expression;
import com.viskan.payloadbuilder.parser.QueryParser;

import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.IntStream;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.RunnerException;

@State(Scope.Thread)
public class BenchmarkTest
{
    Expression e;
    TableAlias ta;
    Row r;
    Function<Row, Object> func;
    EvaluationContext context;

    @Setup(Level.Trial)
    public void setup()
    {
        e = new QueryParser().parseExpression(new CatalogRegistry(), "a.filter(a -> a % 2 = 0)");
        ta = TableAlias.of(null, "t", "a");
        ta.setColumns(new String[] { "a" });
        r = Row.of(ta, 0, new Object[] { IntStream.range(0, 100000).mapToObj(i -> i).collect(toList()) } );
        func = new CodeGenerator().generateFunction(ta, e);
        context = new EvaluationContext();
    }
    
    
    @Benchmark
    @Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
    @Fork(1)
    public void test_eval()
    {
        Iterator it = (Iterator) e.eval(context, r);
        while (it.hasNext())
        {
            it.next();
        }
    }
    
    @Benchmark
    @Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
    @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
    @Fork(1)
    public void test_codegen()
    {
        Iterator it = (Iterator) func.apply(r);
        while (it.hasNext())
        {
            it.next();
        }
    }
    
//    @Benchmark
//    public void test()
//    {
//        
//    }
    
//    @Benchmark
//    @Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
//    @Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
//    @Fork(1)
//    public void test_primitives()
//    {
//        int res0 = 1;
//        int res1 = 10;
//        
//        boolean res2 = res0 > res1;
//       
//        int res3 = 5;
//        int res4 = 20;
//        
//        boolean res5 = res3 < res4;
//        
//        boolean res6 = res2 && res5;
//    }
//    
//    @Benchmark
//    @Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
//    @Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
//    @Fork(1)
//    public void test_object_compare()
//    {
//        // 1 > 10 && 5 < 20
//        
//        Object res0 = 1;
//        Object res1 = 10;
//        
//        boolean res2 = BaseExpression.gt(res0, res1);
//        
//        
//        Object res3 = 5;
//        Object res4 = 20;
//        
//        boolean res5 = BaseExpression.lt(res3, res4);
//        
//        boolean res6 = res2 && res5;
//    }
//    
//    @Benchmark
//    @Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
//    @Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
//    @Fork(1)
//    public void test_object_compare_correct_overload()
//    {
//        // 1 > 10 && 5 < 20
//        
//        Object res0 = 1;
//        Object res1 = 10;
//        
//        boolean res2 = false;
//        if (res0 instanceof Number && res1 instanceof Number)
//        {
//            res2 = BaseExpression.compare((Number) res0, (Number) res1, BaseExpression.GREATER_THAN);
//        }
//        
//        Object res3 = 5;
//        Object res4 = 20;
//        
//        boolean res5 = false;
//        if (res3 instanceof Number && res4 instanceof Number)
//        {
//            res5 = BaseExpression.compare((Number) res3, (Number) res4, BaseExpression.GREATER_THAN);
//        }
//        
//        boolean res6 = res2 && res5;
//        
//    }
//    
//    @Benchmark
//    @Warmup(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
//    @Measurement(iterations = 5, time = 10, timeUnit = TimeUnit.SECONDS)
//    @Fork(1)
//    public void test_object_instance_of()
//    {
//        Object res0 = 1;
//        Object res1 = 10;
//
//        boolean res2 = false;
//        if (res0 instanceof Number && res1 instanceof Number)
//        {
//            if (res0 instanceof Double || res1 instanceof Double)
//            {
//                res2 = ((Number) res0).doubleValue() > ((Number) res1).doubleValue();
//            }
//            else if (res0 instanceof Long || res0 instanceof Long)
//            {
//                res2 = ((Number) res0).longValue() > ((Number) res1).longValue();
//            }
//        }
//        
//        Object res3 = 5;
//        Object res4 = 20;
//        
//        boolean res5 = false;
//        if (res3 instanceof Number && res4 instanceof Number)
//        {
//            if (res3 instanceof Double || res4 instanceof Double)
//            {
//                res5 = ((Number) res3).doubleValue() < ((Number) res4).doubleValue();
//            }
//            else if (res3 instanceof Long || res4 instanceof Long)
//            {
//                res5 = ((Number) res3).longValue() < ((Number) res4).longValue();
//            }
//        }
//        
//        boolean res6 = res2 && res5;
//    }
    
    public static void main(String[] args) throws RunnerException, IOException
    {
//        BenchmarkTest bt = new BenchmarkTest();
//        bt.setup();
//        System.out.println(IteratorUtils.toList((Iterator) bt.test_eval()));
//        System.out.println(IteratorUtils.toList((Iterator) bt.test_codegen()));
        
        org.openjdk.jmh.Main.main(args);
    }
}
