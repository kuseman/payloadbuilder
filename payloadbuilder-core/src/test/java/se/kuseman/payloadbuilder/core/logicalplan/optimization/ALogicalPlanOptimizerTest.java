package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static se.kuseman.payloadbuilder.api.QualifiedName.of;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.core.QuerySession;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.logicalplan.ALogicalPlanTest;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.statement.LogicalSelectStatement;

/** Base class for logicla plan optimization tests */
public abstract class ALogicalPlanOptimizerTest extends ALogicalPlanTest
{
    private static final String TEST = "test";

    private final SchemaResolver schemaResolver = new SchemaResolver();
    private final ColumnResolver columnResolver = new ColumnResolver();

    protected TableSourceReference table = new TableSourceReference("es", of("table"), "t");
    protected TableSourceReference tableA = new TableSourceReference("es", of("tableA"), "a");
    protected TableSourceReference tableB = new TableSourceReference("es", of("tableB"), "b");

    // CSOFF
    protected TableSourceReference sTableA = new TableSourceReference("", of("stableA"), "a");
    protected TableSourceReference sTableB = new TableSourceReference("", of("stableB"), "b");
    protected TableSourceReference sTableC = new TableSourceReference("", of("stableC"), "c");
    protected TableSourceReference sTableD = new TableSourceReference("", of("stableD"), "d");
    // CSON

    protected Schema schemaSTableA = Schema.of(col("col1", Type.Int), col("col2", Type.String), col("col3", Type.Float));
    protected Schema schemaSTableB = Schema.of(col("col1", Type.Boolean), col("col2", Type.String), col("col3", Type.Float));
    protected Schema schemaSTableC = Schema.of(col("col1", Type.Double), col("col2", Type.Boolean), col("col3", Type.Long));
    protected Schema schemaSTableD = Schema.of(Column.of(sTableD.column("col1"), ResolvedType.of(Type.Double)), Column.of(sTableD.column("col6"), ResolvedType.of(Type.String)));

    private final CatalogRegistry registry = new CatalogRegistry();
    protected final QuerySession session = new QuerySession(registry);
    private final Catalog asteriskCatalog = new Catalog(TEST)
    {
        @Override
        public TableSchema getTableSchema(IQuerySession session, String catalogAlias, QualifiedName table)
        {
            String t = table.toDotDelimited()
                    .toLowerCase();
            if (t.equals("stablea"))
            {
                return new TableSchema(schemaSTableA);
            }
            else if (t.equals("stableb"))
            {
                return new TableSchema(schemaSTableB);
            }
            else if (t.equals("stablec"))
            {
                return new TableSchema(schemaSTableC);
            }
            else if (t.equalsIgnoreCase("stableD"))
            {
                return new TableSchema(schemaSTableD);
            }
            return TableSchema.EMPTY;
        };
    };
    protected ExecutionContext context = new ExecutionContext(session);

    public ALogicalPlanOptimizerTest()
    {
        registry.registerCatalog(TEST, asteriskCatalog);
        session.setDefaultCatalogAlias(TEST);
    }

    protected ILogicalPlan getSchemaResolvedPlan(String query)
    {
        ALogicalPlanOptimizer.Context ctx = schemaResolver.createContext(context);
        return schemaResolver.optimize(ctx, s(query));
    }

    protected ILogicalPlan getColumnResolvedPlan(String query)
    {
        ALogicalPlanOptimizer.Context ctx = columnResolver.createContext(context);
        return columnResolver.optimize(ctx, getSchemaResolvedPlan(query));
    }

    protected ILogicalPlan s(String query)
    {
        LogicalSelectStatement stm = (LogicalSelectStatement) PARSER.parseSelect(query);
        return stm.getSelect();
    }
}
