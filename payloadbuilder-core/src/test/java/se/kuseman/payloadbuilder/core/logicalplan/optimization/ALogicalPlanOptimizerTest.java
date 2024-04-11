package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static se.kuseman.payloadbuilder.api.QualifiedName.of;

import java.util.List;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
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
    static final String TEST = "test";
    static final String STABLE_E_ID = "stableEId";

    private final SchemaResolver schemaResolver = new SchemaResolver();
    private final ColumnResolver columnResolver = new ColumnResolver();

    protected TableSourceReference table = new TableSourceReference(0, "es", of("table"), "t");
    protected TableSourceReference tableA = new TableSourceReference(1, "es", of("tableA"), "a");
    protected TableSourceReference tableB = new TableSourceReference(2, "es", of("tableB"), "b");

    // CSOFF
    protected TableSourceReference sTableA = new TableSourceReference(0, "", of("stableA"), "a");
    protected TableSourceReference sTableB = new TableSourceReference(1, "", of("stableB"), "b");
    protected TableSourceReference sTableC = new TableSourceReference(2, "", of("stableC"), "c");
    protected TableSourceReference sTableD = new TableSourceReference(3, "", of("stableD"), "d");

    // CSON
    protected TableSourceReference sTableE(int id)
    {
        return new TableSourceReference(id, "", of("stableE"), "e");
    }

    protected Schema schemaSTableA = Schema.of(col("col1", Type.Int), col("col2", Type.String), col("col3", Type.Float));
    protected Schema schemaSTableB = Schema.of(col("col1", Type.Boolean), col("col2", Type.String), col("col3", Type.Float));
    protected Schema schemaSTableC = Schema.of(col("col1", Type.Double), col("col2", Type.Boolean), col("col3", Type.Long));
    protected Schema schemaSTableD = Schema.of(CoreColumn.of("col1", ResolvedType.of(Type.Double), sTableD), CoreColumn.of("col6", ResolvedType.of(Type.String), sTableD));

    //@formatter:off
    protected Schema schemaSTableE(int id)
    {
        TableSourceReference sTableE = sTableE(id);
        return Schema.of(
              CoreColumn.of("col1", ResolvedType.of(Type.Double), sTableE),
              CoreColumn.of("col3", ResolvedType.table(
                      Schema.of(
                              Column.of("nCol1", Type.Int),
                              Column.of("nCol2", Type.String)
                              )), sTableE),
              CoreColumn.of("col6", ResolvedType.of(Type.String), sTableE));
    }
    //@formatter:on

    private final CatalogRegistry registry = new CatalogRegistry();
    protected final QuerySession session = new QuerySession(registry);
    private final Catalog asteriskCatalog = new Catalog(TEST)
    {
        @Override
        public TableSchema getTableSchema(IQuerySession session, String catalogAlias, QualifiedName table, List<Option> options)
        {
            Integer stablEId = (Integer) session.getCatalogProperty(catalogAlias, STABLE_E_ID)
                    .valueAsObject(0);

            String t = table.toString()
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
                return new TableSchema(schemaSTableE(stablEId));
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
