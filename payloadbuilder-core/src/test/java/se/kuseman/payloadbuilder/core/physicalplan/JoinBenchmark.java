package se.kuseman.payloadbuilder.core.physicalplan;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.ComparisonExpression;

//CSOFF
@BenchmarkMode({ Mode.AverageTime })
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(
        value = 1,
        warmups = 2,
        jvmArgs = { "-Xms4G", "-Xmx4G" })
@Measurement(
        iterations = 3,
        time = 5)
@Warmup(
        iterations = 2,
        time = 5)
public class JoinBenchmark extends APhysicalPlanTest
{
    private static final Schema OUTER = Schema.of(Column.of("col1", Column.Type.Int));
    private static final Schema INNER = Schema.of(Column.of("col2", Column.Type.Int));
    private static final TableSourceReference TS_OUTER = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("outer"), "");
    private static final TableSourceReference TS_INNER = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("inner"), "");

    private static final IExpression COL1 = ColumnExpression.Builder.of("col1", ResolvedType.of(Column.Type.Int))
            .withOrdinal(0)
            .build();

    private static final IExpression COL2 = ColumnExpression.Builder.of("col2", ResolvedType.of(Column.Type.Int))
            .withOrdinal(1)
            .build();

    // a.col1 = b.col2
    private static final IExpression PREDICATE = new ComparisonExpression(IComparisonExpression.Type.EQUAL, COL1, COL2);

    //@formatter:off
    @Param({ 
        "100000;1000000;500",
        "1000000;100000;500",

        "100000;1000000;1000",
        "1000000;100000;1000",
        
        "100000;1000000;10000",
        "1000000;100000;10000",
    })
    //@formatter:on
    private String testData;

    private TupleVector outerVector;
    private TupleVector innerVector;

    @Setup
    public void setup()
    {
        Random r = new Random();

        String[] parts = testData.split(";");

        int outerSize = Integer.parseInt(parts[0]);
        int innerSize = Integer.parseInt(parts[1]);
        int randomizeBound = Integer.parseInt(parts[2]);

        int[] outerValues = new int[outerSize];
        int[] innerValues = new int[innerSize];

        for (int i = 0; i < outerSize; i++)
        {
            outerValues[i] = r.nextInt(randomizeBound);
        }
        for (int i = 0; i < innerSize; i++)
        {
            innerValues[i] = r.nextInt(randomizeBound);
        }

        outerVector = TupleVector.of(OUTER, List.of(new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Int);
            }

            @Override
            public int size()
            {
                return outerSize;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public int getInt(int row)
            {
                return outerValues[row];
            }
        }));
        innerVector = TupleVector.of(INNER, List.of(new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.Int);
            }

            @Override
            public int size()
            {
                return innerSize;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public int getInt(int row)
            {
                return innerValues[row];
            }
        }));
    }

    @Benchmark
    public void nestedLoopInnerJoin(Blackhole bh)
    {
        //@formatter:off
        NestedLoop plan = NestedLoop.innerJoin(
                0,
                scan(schemaDS(() ->{}, outerVector), TS_OUTER, OUTER, 500),
                scan(schemaDS(() ->{}, innerVector), TS_INNER, INNER, 500),
                (tv, ctx) -> PREDICATE.eval(tv, ctx),
                null, false);
        //@formatter:on
        consume(plan.execute(context), bh);
    }

    @Benchmark
    public void hashMatchInnerJoin(Blackhole bh)
    {
        //@formatter:off
        HashMatch plan = new HashMatch(
                0,
                scan(schemaDS(() ->{}, outerVector), TS_OUTER, OUTER, 1500),
                scan(schemaDS(() ->{}, innerVector), TS_INNER, INNER, 1500),
                List.of(COL1),
                List.of(COL2),
                (tv, ctx) -> PREDICATE.eval(tv, ctx),
                null,
                false,
                false);
        //@formatter:on
        consume(plan.execute(context), bh);
    }

    private void consume(TupleIterator it, Blackhole bh)
    {
        int batch = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();
            batch++;
            int size = next.getRowCount();

            // System.out.println(size);

            ValueVector column = next.getColumn(0);
            for (int j = 0; j < size; j++)
            {
                bh.consume(column.getInt(j));
            }
            column = next.getColumn(1);
            for (int j = 0; j < size; j++)
            {
                bh.consume(column.getInt(j));
            }
        }
        it.close();

        System.out.println(batch);
    }

    public static void main(String[] args) throws RunnerException
    {
        // JoinBenchmark b = new JoinBenchmark();
        // b.testData = "1000000;100000;500";
        // b.setup();
        //
        // long time = System.currentTimeMillis();
        // b.hashMatchInnerJoin(new Blackhole("Today's password is swordfish. I understand instantiating Blackholes directly is dangerous."));
        // System.out.println(System.currentTimeMillis() - time);

        Options opt = new OptionsBuilder().include(JoinBenchmark.class.getSimpleName())
                .addProfiler(GCProfiler.class)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    /* @formatter:off
     2024-03-22
     As expected hash match allocates alot be is really fast compared to nested loop
     NOTES!
     - Hash match 100000;1000000;500 (10715,268)
                  1000000;100000;500 (6272,225)
       Smaller outer is slow compared to smaller inner, why?
     
     - Hash match 1000000;100000;1000 allocates huge amounts (625,622  MB/sec)
                  100000;1000000;1000 allocates a third (209,805)
       Smaller outer is worse here aswell, why?
     
     
Benchmark                                                       (testData)  Mode  Cnt           Score        Error   Units
JoinBenchmark.hashMatchInnerJoin                        100000;1000000;500  avgt    3       10715,268 ±   7320,284   ms/op
JoinBenchmark.hashMatchInnerJoin:gc.alloc.rate          100000;1000000;500  avgt    3         277,669 ±    193,147  MB/sec
JoinBenchmark.hashMatchInnerJoin:gc.alloc.rate.norm     100000;1000000;500  avgt    3  3116920026,667 ±    337,057    B/op
JoinBenchmark.hashMatchInnerJoin:gc.count               100000;1000000;500  avgt    3           9,000               counts
JoinBenchmark.hashMatchInnerJoin:gc.time                100000;1000000;500  avgt    3          28,000                   ms
JoinBenchmark.hashMatchInnerJoin                        1000000;100000;500  avgt    3        6272,225 ±   9553,547   ms/op
JoinBenchmark.hashMatchInnerJoin:gc.alloc.rate          1000000;100000;500  avgt    3         277,234 ±    410,389  MB/sec
JoinBenchmark.hashMatchInnerJoin:gc.alloc.rate.norm     1000000;100000;500  avgt    3  1815195752,000 ±      0,001    B/op
JoinBenchmark.hashMatchInnerJoin:gc.count               1000000;100000;500  avgt    3           3,000               counts
JoinBenchmark.hashMatchInnerJoin:gc.time                1000000;100000;500  avgt    3          10,000                   ms
JoinBenchmark.hashMatchInnerJoin                       100000;1000000;1000  avgt    3        5028,553 ±   2388,196   ms/op
JoinBenchmark.hashMatchInnerJoin:gc.alloc.rate         100000;1000000;1000  avgt    3         440,493 ±    209,805  MB/sec
JoinBenchmark.hashMatchInnerJoin:gc.alloc.rate.norm    100000;1000000;1000  avgt    3  2321693778,667 ±   2864,981    B/op
JoinBenchmark.hashMatchInnerJoin:gc.count              100000;1000000;1000  avgt    3          10,000               counts
JoinBenchmark.hashMatchInnerJoin:gc.time               100000;1000000;1000  avgt    3          35,000                   ms
JoinBenchmark.hashMatchInnerJoin                       1000000;100000;1000  avgt    3        3881,266 ±   8160,500   ms/op
JoinBenchmark.hashMatchInnerJoin:gc.alloc.rate         1000000;100000;1000  avgt    3         279,347 ±    622,763  MB/sec
JoinBenchmark.hashMatchInnerJoin:gc.alloc.rate.norm    1000000;100000;1000  avgt    3  1126256080,000 ±      0,001    B/op
JoinBenchmark.hashMatchInnerJoin:gc.count              1000000;100000;1000  avgt    3           6,000               counts
JoinBenchmark.hashMatchInnerJoin:gc.time               1000000;100000;1000  avgt    3          19,000                   ms
JoinBenchmark.hashMatchInnerJoin                      100000;1000000;10000  avgt    3         868,006 ±    312,529   ms/op
JoinBenchmark.hashMatchInnerJoin:gc.alloc.rate        100000;1000000;10000  avgt    3         450,991 ±    162,852  MB/sec
JoinBenchmark.hashMatchInnerJoin:gc.alloc.rate.norm   100000;1000000;10000  avgt    3   410397992,889 ±     56,176    B/op
JoinBenchmark.hashMatchInnerJoin:gc.count             100000;1000000;10000  avgt    3           2,000               counts
JoinBenchmark.hashMatchInnerJoin:gc.time              100000;1000000;10000  avgt    3           7,000                   ms
JoinBenchmark.hashMatchInnerJoin                      1000000;100000;10000  avgt    3         416,073 ±    102,031   ms/op
JoinBenchmark.hashMatchInnerJoin:gc.alloc.rate        1000000;100000;10000  avgt    3        2565,053 ±    625,622  MB/sec
JoinBenchmark.hashMatchInnerJoin:gc.alloc.rate.norm   1000000;100000;10000  avgt    3  1119013288,479 ±     34,570    B/op
JoinBenchmark.hashMatchInnerJoin:gc.count             1000000;100000;10000  avgt    3          38,000               counts
JoinBenchmark.hashMatchInnerJoin:gc.time              1000000;100000;10000  avgt    3          59,000                   ms
JoinBenchmark.nestedLoopInnerJoin                       100000;1000000;500  avgt    3       23386,912 ±   8469,968   ms/op
JoinBenchmark.nestedLoopInnerJoin:gc.alloc.rate         100000;1000000;500  avgt    3           6,991 ±      2,558  MB/sec
JoinBenchmark.nestedLoopInnerJoin:gc.alloc.rate.norm    100000;1000000;500  avgt    3   171400765,333 ±    337,057    B/op
JoinBenchmark.nestedLoopInnerJoin:gc.count              100000;1000000;500  avgt    3             ≈ 0               counts
JoinBenchmark.nestedLoopInnerJoin                       1000000;100000;500  avgt    3       22311,735 ±   4667,644   ms/op
JoinBenchmark.nestedLoopInnerJoin:gc.alloc.rate         1000000;100000;500  avgt    3           7,328 ±      1,524  MB/sec
JoinBenchmark.nestedLoopInnerJoin:gc.alloc.rate.norm    1000000;100000;500  avgt    3   171427133,333 ±    337,057    B/op
JoinBenchmark.nestedLoopInnerJoin:gc.count              1000000;100000;500  avgt    3             ≈ 0               counts
JoinBenchmark.nestedLoopInnerJoin                      100000;1000000;1000  avgt    3       20826,524 ±  28391,803   ms/op
JoinBenchmark.nestedLoopInnerJoin:gc.alloc.rate        100000;1000000;1000  avgt    3           7,431 ±      9,850  MB/sec
JoinBenchmark.nestedLoopInnerJoin:gc.alloc.rate.norm   100000;1000000;1000  avgt    3   161698120,000 ±      0,001    B/op
JoinBenchmark.nestedLoopInnerJoin:gc.count             100000;1000000;1000  avgt    3             ≈ 0               counts
JoinBenchmark.nestedLoopInnerJoin                      1000000;100000;1000  avgt    3       22735,209 ±  34678,068   ms/op
JoinBenchmark.nestedLoopInnerJoin:gc.alloc.rate        1000000;100000;1000  avgt    3           6,813 ±      9,939  MB/sec
JoinBenchmark.nestedLoopInnerJoin:gc.alloc.rate.norm   1000000;100000;1000  avgt    3   161691349,333 ±   9526,322    B/op
JoinBenchmark.nestedLoopInnerJoin:gc.count             1000000;100000;1000  avgt    3             ≈ 0               counts
JoinBenchmark.nestedLoopInnerJoin                     100000;1000000;10000  avgt    3       22746,661 ±  18428,823   ms/op
JoinBenchmark.nestedLoopInnerJoin:gc.alloc.rate       100000;1000000;10000  avgt    3           6,421 ±      5,333  MB/sec
JoinBenchmark.nestedLoopInnerJoin:gc.alloc.rate.norm  100000;1000000;10000  avgt    3   152948658,667 ± 275880,967    B/op
JoinBenchmark.nestedLoopInnerJoin:gc.count            100000;1000000;10000  avgt    3             ≈ 0               counts
JoinBenchmark.nestedLoopInnerJoin                     1000000;100000;10000  avgt    3       22196,972 ±  23116,444   ms/op
JoinBenchmark.nestedLoopInnerJoin:gc.alloc.rate       1000000;100000;10000  avgt    3           6,919 ±      7,422  MB/sec
JoinBenchmark.nestedLoopInnerJoin:gc.alloc.rate.norm  1000000;100000;10000  avgt    3   160687512,000 ±      0,001    B/op
JoinBenchmark.nestedLoopInnerJoin:gc.count            1000000;100000;10000  avgt    3             ≈ 0               counts
     @formatter:on
     */
}
// CSON
