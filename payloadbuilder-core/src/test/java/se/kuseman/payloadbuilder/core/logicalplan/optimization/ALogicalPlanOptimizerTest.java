package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static se.kuseman.payloadbuilder.api.QualifiedName.of;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
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

    protected TableSourceReference sTableE = new TableSourceReference("", of("stableE"), "e");
    // CSON

    protected ColumnReference stACol1 = new ColumnReference(sTableA, "col1", ColumnReference.Type.REGULAR);
    protected ColumnReference stACol2 = new ColumnReference(sTableA, "col2", ColumnReference.Type.REGULAR);
    protected ColumnReference stACol3 = new ColumnReference(sTableA, "col3", ColumnReference.Type.REGULAR);
    protected ColumnReference stBCol1 = new ColumnReference(sTableB, "col1", ColumnReference.Type.REGULAR);
    protected ColumnReference stBCol2 = new ColumnReference(sTableB, "col2", ColumnReference.Type.REGULAR);
    protected ColumnReference stBCol3 = new ColumnReference(sTableB, "col3", ColumnReference.Type.REGULAR);

    protected Schema schemaSTableA = Schema.of(col("col1", Type.Int), col("col2", Type.String), col("col3", Type.Float));
    protected Schema schemaSTableB = Schema.of(col("col1", Type.Boolean), col("col2", Type.String), col("col3", Type.Float));
    protected Schema schemaSTableC = Schema.of(col("col1", Type.Double), col("col2", Type.Boolean), col("col3", Type.Long));
    protected Schema schemaSTableD = Schema.of(CoreColumn.of(sTableD.column("col1"), ResolvedType.of(Type.Double)), CoreColumn.of(sTableD.column("col6"), ResolvedType.of(Type.String)));

    //@formatter:off
    protected Schema schemaSTableE = Schema.of(
            CoreColumn.of(sTableE.column("col1"), ResolvedType.of(Type.Double)),
            CoreColumn.of(sTableE.column("col3"), ResolvedType.table(
                    Schema.of(
                            Column.of("nCol1", Type.Int),
                            Column.of("nCol2", Type.String)
                            ))),
            CoreColumn.of(sTableE.column("col6"), ResolvedType.of(Type.String)));
    //@formatter:on

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
            else if (t.equalsIgnoreCase("stableE"))
            {
                return new TableSchema(schemaSTableE);
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
