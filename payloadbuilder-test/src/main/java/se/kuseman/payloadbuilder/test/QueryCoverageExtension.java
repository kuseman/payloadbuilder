package se.kuseman.payloadbuilder.test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.extension.AfterAllCallback;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ExtensionContext.Namespace;
import org.junit.jupiter.api.extension.ExtensionContext.Store;

import se.kuseman.payloadbuilder.core.execution.PlanRule;
import se.kuseman.payloadbuilder.core.execution.PlanRuleRegistry;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageData;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageData.BranchCoverage;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageData.ConditionCoverage;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageData.OperatorCoverage;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageRegistry;

/**
 * JUnit 5 extension for SQL query coverage tracking and compile-time plan rule enforcement. Use {@code @ExtendWith} for a console summary only, or {@code @RegisterExtension static} to get a reference
 * needed for {@link CoverageTestFactory#from(CoverageAccumulator)} in a {@code @TestFactory @Order(Integer.MAX_VALUE)} method. A JSON report is written to the build output directory after all tests
 * complete (see {@link #resolveOutputDir()} for the per-tool convention). Plan rules (e.g. no full table scans) are registered via {@link #addRule(PlanRule)} and are checked at compile time; a
 * {@link se.kuseman.payloadbuilder.core.execution.PlanRuleViolationException} propagates as a normal test failure on the {@code @Test} method that triggered the compile.
 */
public class QueryCoverageExtension implements BeforeAllCallback, AfterAllCallback
{
    private static final Namespace NAMESPACE = Namespace.create(QueryCoverageExtension.class);
    private static final String ACCUMULATOR_KEY = "accumulator";

    private CoverageAccumulator accumulator;
    private final List<PlanRule> planRules = new ArrayList<>();
    private final Map<String, String> querySources = new LinkedHashMap<>();

    /**
     * Registers the source file path for a named query. When present, {@link CoverageReport} generates an annotated {@code .annotated.sql} file alongside the JSON report, showing each SQL line marked
     * with {@code ✓} (covered), {@code ✗} (never executed), or {@code ~} (partially covered). The name must match the one passed to {@code Payloadbuilder.compile(session, sql, queryName)}.
     * Multi-statement files compiled as {@code name}, {@code name#1}, … are automatically merged into one annotated file. Returns {@code this} for fluent chaining.
     */
    public QueryCoverageExtension registerSource(String queryName, String sourceFile)
    {
        querySources.put(queryName, sourceFile);
        return this;
    }

    /**
     * Registers a {@link PlanRule} that will be checked against every compiled physical plan on this thread while the extension is active. Call before tests run — typically in the field initialiser
     * or in {@code @BeforeAll}. Returns {@code this} for fluent chaining.
     *
     * <pre>
     * {@code @RegisterExtension}
     * static final QueryCoverageExtension coverage = new QueryCoverageExtension()
     *     .addRule(new NoFullTableScanRule());
     * </pre>
     */
    public QueryCoverageExtension addRule(PlanRule rule)
    {
        planRules.add(rule);
        return this;
    }

    @Override
    public void beforeAll(ExtensionContext context)
    {
        accumulator = new CoverageAccumulator(context.getDisplayName());
        getStore(context).put(ACCUMULATOR_KEY, accumulator);
        QueryCoverageRegistry.register(accumulator);
        planRules.forEach(PlanRuleRegistry::register);
    }

    @Override
    public void afterAll(ExtensionContext context)
    {
        planRules.forEach(PlanRuleRegistry::deregister);
        QueryCoverageRegistry.deregister(accumulator);
        CoverageReport.write(accumulator, resolveOutputDir(), querySources);
        publishCoverageSummary(context, accumulator);
    }

    /** Returns the accumulator holding all coverage data collected so far. Non-null after {@link #beforeAll} has run. */
    public CoverageAccumulator getAccumulator()
    {
        return accumulator;
    }

    private static void publishCoverageSummary(ExtensionContext context, CoverageAccumulator acc)
    {
        long totalOperators = 0;
        long uncoveredOperators = 0;
        long totalBranches = 0;
        long uncoveredBranches = 0;
        long totalConditions = 0;
        long uncoveredConditions = 0;

        for (QueryCoverageData data : acc.getCollected())
        {
            for (OperatorCoverage op : data.getOperators())
            {
                totalOperators++;
                if (!op.covered)
                {
                    uncoveredOperators++;
                }
            }
            for (BranchCoverage br : data.getBranches())
            {
                for (long hits : br.whenHits)
                {
                    totalBranches++;
                    if (hits == 0)
                    {
                        uncoveredBranches++;
                    }
                }
                totalBranches++;
                if (br.elseHits == 0)
                {
                    uncoveredBranches++;
                }
            }
            for (ConditionCoverage cond : data.getConditions())
            {
                totalConditions += 2;
                if (cond.trueHits == 0)
                {
                    uncoveredConditions++;
                }
                if (cond.falseHits == 0)
                {
                    uncoveredConditions++;
                }
            }
        }

        if (totalOperators == 0)
        {
            return;
        }

        String summary = String.format("operators: %d/%d covered, branches: %d/%d covered, conditions: %d/%d covered", totalOperators - uncoveredOperators, totalOperators,
                totalBranches - uncoveredBranches, totalBranches, totalConditions - uncoveredConditions, totalConditions);
        context.publishReportEntry(Map.of("query-coverage", summary));
    }

    private static Store getStore(ExtensionContext context)
    {
        return context.getStore(NAMESPACE);
    }

    /**
     * Resolves the coverage output directory using build-tool conventions, checked in order:
     * <ol>
     * <li>System property {@code payloadbuilder.coverage.outputDir} — explicit override for any tool or CI environment.</li>
     * <li>Maven via Surefire: {@code maven.projectBasedir} is set → {@code <basedir>/target/coverage}.</li>
     * <li>Maven in Eclipse (no Surefire): {@code pom.xml} found in {@code user.dir} → {@code <user.dir>/target/coverage}.</li>
     * <li>Gradle: {@code build.gradle} or {@code build.gradle.kts} found in {@code user.dir} → {@code <user.dir>/build/coverage}.</li>
     * <li>Ant: {@code build.xml} found in {@code user.dir} → {@code <user.dir>/build/coverage}.</li>
     * <li>Fallback: {@code <user.dir>/target/coverage}.</li>
     * </ol>
     */
    static Path resolveOutputDir()
    {
        String explicit = System.getProperty("payloadbuilder.coverage.outputDir");
        if (explicit != null
                && !explicit.isEmpty())
        {
            return Paths.get(explicit);
        }
        String mavenBasedir = System.getProperty("maven.projectBasedir");
        if (mavenBasedir != null)
        {
            return Paths.get(mavenBasedir, "target", "coverage");
        }
        Path userDir = Paths.get(System.getProperty("user.dir", "."));
        if (Files.exists(userDir.resolve("pom.xml")))
        {
            return userDir.resolve(Paths.get("target", "coverage"));
        }
        if (Files.exists(userDir.resolve("build.gradle"))
                || Files.exists(userDir.resolve("build.gradle.kts")))
        {
            return userDir.resolve(Paths.get("build", "coverage"));
        }
        if (Files.exists(userDir.resolve("build.xml")))
        {
            return userDir.resolve(Paths.get("build", "coverage"));
        }
        return userDir.resolve(Paths.get("target", "coverage"));
    }
}
