/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.kuse.payloadbuilder.core.codegen;

public class BenchmarkTest
{
    //    Expression e;
    //    TableAlias ta;
    //    Row r;
    //    Function<Row, Object> func;
    //    ExecutionContext context;
    //
    //    @Setup(Level.Trial)
    //    public void setup()
    //    {
    //        e = new QueryParser().parseExpression("a.filter(a -> a % 2 = 0)");
    //        ta = TableAlias.of(null, "t", "a");
    //        ta.setColumns(new String[] { "a" });
    //        r = Row.of(ta, 0, new Object[] { IntStream.range(0, 100000).mapToObj(i -> i).collect(toList()) } );
    //        func = new CodeGenerator().generateFunction(ta, e);
    //        context = new ExecutionContext();
    //    }
    //    
    //    
    //    @Benchmark
    //    @Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
    //    @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
    //    @Fork(1)
    //    public void test_eval()
    //    {
    //        Iterator it = (Iterator) e.eval(context, r);
    //        while (it.hasNext())
    //        {
    //            it.next();
    //        }
    //    }
    //    
    //    @Benchmark
    //    @Warmup(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
    //    @Measurement(iterations = 3, time = 10, timeUnit = TimeUnit.SECONDS)
    //    @Fork(1)
    //    public void test_codegen()
    //    {
    //        Iterator it = (Iterator) func.apply(r);
    //        while (it.hasNext())
    //        {
    //            it.next();
    //        }
    //    }

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

    //    public static void main(String[] args) throws RunnerException, IOException
    //    {
    ////        BenchmarkTest bt = new BenchmarkTest();
    ////        bt.setup();
    ////        System.out.println(IteratorUtils.toList((Iterator) bt.test_eval()));
    ////        System.out.println(IteratorUtils.toList((Iterator) bt.test_codegen()));
    //        
    //        org.openjdk.jmh.Main.main(args);
    //    }
}
