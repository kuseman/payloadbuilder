package se.kuseman.payloadbuilder.test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.core.execution.QueryCoverageData;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageData.BranchCoverage;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageData.ConditionCoverage;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageData.OperatorCoverage;

/** Writes aggregate JSON and annotated SQL coverage reports for a test class run. */
public class CoverageReport
{
    private CoverageReport()
    {
    }

    /**
     * Write coverage reports to {@code outputDir}. Always produces a JSON file. For each query that has a matching entry in {@code querySources}, also produces a {@code .annotated.sql} file showing
     * every SQL line prefixed with {@code ✓} (covered), {@code ✗} (never executed), {@code ~} (partially covered), or a blank when no coverage data exists for that line.
     *
     * @param accumulator the collected coverage data
     * @param outputDir directory where report files are written
     * @param querySources map from compile-time query name to source file path; multi-statement keys ({@code name#1}, …) match via prefix
     */
    public static void write(CoverageAccumulator accumulator, Path outputDir, Map<String, String> querySources)
    {
        List<QueryCoverageData> collected = accumulator.getCollected();
        if (collected.isEmpty())
        {
            return;
        }

        try
        {
            Files.createDirectories(outputDir);
            String safeName = accumulator.getTestClassName()
                    .replace('.', '_');
            Path reportFile = outputDir.resolve(safeName + "-coverage.json");
            String json = buildJson(accumulator.getTestClassName(), collected);
            Files.write(reportFile, json.getBytes(StandardCharsets.UTF_8));
        }
        catch (IOException ex)
        {
            throw new UncheckedIOException("Failed to write JSON coverage report", ex);
        }

        if (querySources.isEmpty())
        {
            return;
        }

        // Group collected data by base query name (strip the #N suffix used for multi-statement files)
        Map<String, List<QueryCoverageData>> byBaseName = new HashMap<>();
        for (QueryCoverageData data : collected)
        {
            String baseName = baseName(data.getQueryKey());
            byBaseName.computeIfAbsent(baseName, k -> new ArrayList<>())
                    .add(data);
        }

        for (Map.Entry<String, String> entry : querySources.entrySet())
        {
            List<QueryCoverageData> matching = byBaseName.get(entry.getKey());
            if (matching == null
                    || matching.isEmpty())
            {
                continue;
            }
            Path sourceFile = resolveSourcePath(entry.getValue());
            if (!Files.exists(sourceFile))
            {
                continue;
            }
            try
            {
                String safeName = accumulator.getTestClassName()
                        .replace('.', '_');
                String safeKey = entry.getKey()
                        .replace('#', '-')
                        .replace(' ', '_');
                writeAnnotated(sourceFile, matching, outputDir.resolve(safeName + "-" + safeKey + ".annotated.sql"));
                Files.write(outputDir.resolve(safeName + "-" + safeKey + "-coverage.json"), buildJson(entry.getKey(), matching).getBytes(StandardCharsets.UTF_8));
            }
            catch (IOException ex)
            {
                throw new UncheckedIOException("Failed to write coverage report for " + entry.getKey(), ex);
            }
        }
    }

    // -------------------------------------------------------------------------
    // Annotated SQL writer
    // -------------------------------------------------------------------------

    private static void writeAnnotated(Path sourceFile, List<QueryCoverageData> dataList, Path out) throws IOException
    {
        List<String> lines = Files.readAllLines(sourceFile, StandardCharsets.UTF_8);
        int totalLines = lines.size();
        int lineNumWidth = String.valueOf(totalLines)
                .length();

        // Build line → items maps by merging all statements for this source file
        Map<Integer, List<OperatorCoverage>> opsByLine = new HashMap<>();
        Map<Integer, List<BranchCoverage>> branchesByLine = new HashMap<>();
        Map<Integer, List<ConditionCoverage>> condsByLine = new HashMap<>();

        for (QueryCoverageData data : dataList)
        {
            for (OperatorCoverage op : data.getOperators())
            {
                if (op.location != null
                        && op.location.line() > 0)
                {
                    opsByLine.computeIfAbsent(op.location.line(), k -> new ArrayList<>())
                            .add(op);
                }
            }
            for (BranchCoverage br : data.getBranches())
            {
                if (br.location != null
                        && br.location.line() > 0)
                {
                    branchesByLine.computeIfAbsent(br.location.line(), k -> new ArrayList<>())
                            .add(br);
                }
            }
            for (ConditionCoverage cond : data.getConditions())
            {
                if (cond.location != null
                        && cond.location.line() > 0)
                {
                    condsByLine.computeIfAbsent(cond.location.line(), k -> new ArrayList<>())
                            .add(cond);
                }
            }
        }

        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lines.size(); i++)
        {
            int lineNum = i + 1;
            char status = gutterChar(lineNum, opsByLine, branchesByLine, condsByLine);
            sb.append(status == ' ' ? "  "
                    : status + " ");
            sb.append(String.format("%" + lineNumWidth + "d", lineNum));
            sb.append(" | ");
            sb.append(lines.get(i));
            sb.append(System.lineSeparator());
        }

        Files.write(out, sb.toString()
                .getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Computes the gutter character for one SQL line.
     *
     * <ul>
     * <li>{@code ✓} — all coverage items on this line are fully covered (operator executed, all CASE branches hit, both AND/OR outcomes seen)</li>
     * <li>{@code ✗} — coverage items exist on this line but none had any execution at all</li>
     * <li>{@code ~} — coverage items exist and some executed, but at least one is not fully covered</li>
     * <li>{@code ' '} — no coverage data for this line (e.g. comments, blank lines, SELECT list)</li>
     * </ul>
     */
    private static char gutterChar(int lineNum, Map<Integer, List<OperatorCoverage>> opsByLine, Map<Integer, List<BranchCoverage>> branchesByLine, Map<Integer, List<ConditionCoverage>> condsByLine)
    {
        List<OperatorCoverage> ops = opsByLine.getOrDefault(lineNum, List.of());
        List<BranchCoverage> branches = branchesByLine.getOrDefault(lineNum, List.of());
        List<ConditionCoverage> conds = condsByLine.getOrDefault(lineNum, List.of());

        if (ops.isEmpty()
                && branches.isEmpty()
                && conds.isEmpty())
        {
            return ' ';
        }

        boolean anyExecution = false;
        boolean allFullyCovered = true;

        for (OperatorCoverage op : ops)
        {
            if (op.executionCount > 0)
            {
                anyExecution = true;
            }
            if (!op.covered)
            {
                allFullyCovered = false;
            }
        }
        for (BranchCoverage br : branches)
        {
            long totalHits = br.elseHits;
            for (long h : br.whenHits)
            {
                totalHits += h;
            }
            if (totalHits > 0)
            {
                anyExecution = true;
            }
            boolean fullyHit = br.elseHits > 0
                    && Arrays.stream(br.whenHits)
                            .allMatch(h -> h > 0);
            if (!fullyHit)
            {
                allFullyCovered = false;
            }
        }
        for (ConditionCoverage cond : conds)
        {
            if (cond.trueHits > 0
                    || cond.falseHits > 0)
            {
                anyExecution = true;
            }
            if (cond.trueHits == 0
                    || cond.falseHits == 0)
            {
                allFullyCovered = false;
            }
        }

        if (allFullyCovered)
        {
            return '✓';
        }
        if (!anyExecution)
        {
            return '✗';
        }
        return '~';
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Strips the {@code #N} statement-index suffix added for multi-statement compiles. */
    private static String baseName(String queryKey)
    {
        int hash = queryKey.lastIndexOf('#');
        if (hash > 0)
        {
            String suffix = queryKey.substring(hash + 1);
            if (suffix.chars()
                    .allMatch(Character::isDigit))
            {
                return queryKey.substring(0, hash);
            }
        }
        return queryKey;
    }

    /** Resolves a source file path — absolute or relative to the project base directory. */
    private static Path resolveSourcePath(String sourceFile)
    {
        Path path = Paths.get(sourceFile);
        if (!path.isAbsolute())
        {
            String base = System.getProperty("maven.projectBasedir", System.getProperty("user.dir", "."));
            path = Paths.get(base, sourceFile);
        }
        return path;
    }

    // -------------------------------------------------------------------------
    // JSON report
    // -------------------------------------------------------------------------

    private static String buildJson(String testClassName, List<QueryCoverageData> queries)
    {
        StringBuilder sb = new StringBuilder();
        sb.append("{\"test_class\":\"")
                .append(escapeJson(testClassName))
                .append("\"");
        sb.append(",\"queries\":[");
        boolean firstQuery = true;
        for (QueryCoverageData data : queries)
        {
            if (!firstQuery)
            {
                sb.append(",");
            }
            firstQuery = false;
            sb.append(data.toJson());
        }
        sb.append("]}");
        return sb.toString();
    }

    private static String escapeJson(String s)
    {
        if (s == null)
        {
            return "";
        }
        return s.replace("\\", "\\\\")
                .replace("\"", "\\\"");
    }
}
