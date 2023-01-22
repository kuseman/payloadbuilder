package se.kuseman.payloadbuilder.catalog.es;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static se.kuseman.payloadbuilder.catalog.es.ElasticsearchMetaUtils.getMeta;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;
import se.kuseman.payloadbuilder.catalog.es.ESQueryUtils.SortItemMeta;
import se.kuseman.payloadbuilder.catalog.es.ElasticsearchMetaUtils.MappedProperty;
import se.kuseman.payloadbuilder.catalog.es.ElasticsearchMetaUtils.MappedType;

/** Catalog for querying elastic search */
public class ESCatalog extends Catalog
{
    static final String NAME = "Elastic search";
    public static final String TRUSTCERTIFICATE_KEY = "trustCertificate";
    public static final String CONNECT_TIMEOUT_KEY = "connectTimeout";
    public static final String RECEIVE_TIMEOUT_KEY = "receiveTimeout";
    public static final String AUTH_TYPE_KEY = "authType";
    public static final String AUTH_USERNAME_KEY = "authUsername";
    public static final String AUTH_PASSWORD_KEY = "authPassword";
    public static final String ENDPOINT_KEY = "endpoint";
    public static final String INDEX_KEY = "index";
    static final String SINGLE_TYPE_TABLE_NAME = "_doc";
    private static final int BATCH_SIZE = 250;

    /** Construct a new ES catalog */
    public ESCatalog()
    {
        super("EsCatalog");
        registerFunction(new MustacheCompileFunction(this));
        registerFunction(new SearchFunction(this));
        registerFunction(new MatchFunction(this));
        registerFunction(new QueryFunction(this));
        registerFunction(new CatFunction(this));
        registerFunction(new RenderTemplateFunction(this));
    }

    private static final TableSchema INDICES_SCHEMA = new TableSchema(
            Schema.of(Column.of(SYS_INDICES_TABLE, ResolvedType.of(Type.String)), Column.of(SYS_INDICES_COLUMNS, ResolvedType.of(Type.String))));

    @Override
    public TableSchema getSystemTableSchema(IQuerySession session, String catalogAlias, QualifiedName table)
    {
        if (table.size() == 1)
        {
            String type = table.getLast();
            if (SYS_TABLES.equalsIgnoreCase(type))
            {
                return TableSchema.EMPTY;
            }
            else if (SYS_COLUMNS.equalsIgnoreCase(type))
            {
                return TableSchema.EMPTY;
            }
            else if (SYS_FUNCTIONS.equalsIgnoreCase(type))
            {
                return new TableSchema(SYS_FUNCTIONS_SCHEMA);
            }
            else if (SYS_INDICES.equalsIgnoreCase(type))
            {
                return INDICES_SCHEMA;
            }
        }

        throw new RuntimeException(table + " is not supported");
    }

    @Override
    public IDatasource getSystemTableDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
    {
        String type = table.getLast();
        if (SYS_TABLES.equalsIgnoreCase(type))
        {
            return getTablesDatasource(session, catalogAlias);
        }
        else if (SYS_COLUMNS.equalsIgnoreCase(type))
        {
            return getColumnsDatasource(session, catalogAlias);
        }
        else if (SYS_FUNCTIONS.equalsIgnoreCase(type))
        {
            return (context, options) -> TupleIterator.singleton(getFunctionsTupleVector());
        }
        else if (SYS_INDICES.equalsIgnoreCase(type))
        {
            return getIndicesDatasource(session, catalogAlias);
        }

        throw new RuntimeException(table + " is not supported");
    }

    @Override
    public TableSchema getTableSchema(IQuerySession session, String catalogAlias, QualifiedName table)
    {
        ESType esType = ESType.of(session, catalogAlias, table);
        // All indexed non-free-text fields are index candidates
        Map<String, MappedType> mappedTypes = getMeta(session, catalogAlias, esType.endpoint, esType.index).getMappedTypes();
        Map<String, MappedProperty> properties = Optional.ofNullable(mappedTypes.get(esType.type))
                .map(m -> m.properties)
                .orElse(emptyMap());
        return new TableSchema(Schema.EMPTY, getIndicesInternal(table, properties));
    }

    @Override
    public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
    {
        return getDatasource(session, catalogAlias, table, null, data);
    }

    @Override
    public IDatasource getSeekDataSource(IQuerySession session, String catalogAlias, ISeekPredicate seekPredicate, DatasourceData data)
    {
        return getDatasource(session, catalogAlias, seekPredicate.getIndex()
                .getTable(), seekPredicate, data);
    }

    private IDatasource getDatasource(IQuerySession session, String catalogAlias, QualifiedName table, ISeekPredicate seekPredicate, DatasourceData data)
    {
        ESType esType = ESType.of(session, catalogAlias, table);

        // Fetch analyzed properties
        ElasticsearchMeta meta = getMeta(session, catalogAlias, esType.endpoint, esType.index);
        Map<String, MappedProperty> properties = Optional.ofNullable(meta.getMappedTypes()
                .get(esType.type))
                .map(m -> m.properties)
                .orElse(emptyMap());

        MappedProperty indexProperty = null;

        if (seekPredicate != null)
        {
            if (seekPredicate.getIndexColumns()
                    .size() != 1)
            {
                throw new IllegalArgumentException("Invalid index, catalog only supports single column indices");
            }

            String indexColumn = seekPredicate.getIndexColumns()
                    .get(0);
            // Fetch mapped property for index column. Not needed for _id
            if (!ESDatasource.DOCID.equalsIgnoreCase(indexColumn))
            {
                indexProperty = properties.get(indexColumn);
                // CSOFF
                if (indexProperty == null)
                // CSON
                {
                    throw new IllegalArgumentException("Invalid index column: " + indexColumn);
                }
            }
        }

        List<PropertyPredicate> propertyPredicates = collectPredicates(data.getPredicates(), properties);
        List<SortItemMeta> sortItems = collectSortItems(properties, data.getSortItems());

        return new ESDatasource(data.getNodeId(), meta.getStrategy(), catalogAlias, table, seekPredicate, indexProperty, propertyPredicates, sortItems);
    }

    private List<PropertyPredicate> collectPredicates(List<IPredicate> predicates, Map<String, MappedProperty> properties)
    {
        List<PropertyPredicate> propertyPredicates = new ArrayList<>();
        Iterator<IPredicate> it = predicates.iterator();
        while (it.hasNext())
        {
            IPredicate predicate = it.next();
            if (predicate.getType() == IPredicate.Type.FUNCTION_CALL)
            {
                // TODO: analyze function arguments to properly find a field that is searchable
                // ie. ESC mapping for: http.request.body.content
                // has a field ".text" with type text that should
                // be used in full text search instead
                if (isFullTextSearchPredicate(predicate))
                {
                    propertyPredicates.add(new PropertyPredicate("", predicate, true));
                    it.remove();
                }

                continue;
            }

            QualifiedName qname = predicate.getQualifiedColumn();
            if (qname == null)
            {
                continue;
            }

            String column = qname.toDotDelimited();
            MappedProperty property = properties.get(column);
            // Extra columns only support EQUALS
            if (ESDatasource.INDEX.equals(column)
                    && predicate.getComparisonType() == IComparisonExpression.Type.EQUAL)
            {
                propertyPredicates.add(new PropertyPredicate("_index", predicate, false));
                it.remove();
            }
            else if (ESDatasource.TYPE.equals(column)
                    && predicate.getComparisonType() == IComparisonExpression.Type.EQUAL)
            {
                propertyPredicates.add(new PropertyPredicate("_type", predicate, false));
                it.remove();
            }
            else if (ESDatasource.DOCID.equals(column)
                    && (IComparisonExpression.Type.EQUAL.equals(predicate.getComparisonType())
                            || predicate.getType() == IPredicate.Type.IN))
            {
                propertyPredicates.add(new PropertyPredicate("_id", predicate, false));
                it.remove();
            }
            // TODO: strings only support equals
            else if (property != null)
            {
                String field = property.name;
                if (property.isFreeTextMapping())
                {
                    property = property.getNonFreeTextField();
                    // CSOFF
                    if (property == null)
                    // CSON
                    {
                        continue;
                    }
                    field = property.name;
                }

                propertyPredicates.add(new PropertyPredicate(field, property.nestedPath, predicate, false));
                it.remove();
            }
        }
        return propertyPredicates;
    }

    private boolean isFullTextSearchPredicate(IPredicate pair)
    {
        if (pair.getType() != IPredicate.Type.FUNCTION_CALL)
        {
            return false;
        }
        IFunctionCallExpression functionExpression = pair.getFunctionCallExpression();
        ScalarFunctionInfo functionInfo = functionExpression.getFunctionInfo();
        return functionInfo instanceof MatchFunction
                || functionInfo instanceof QueryFunction;
    }

    private List<SortItemMeta> collectSortItems(Map<String, MappedProperty> properties, List<? extends ISortItem> sortItems)
    {
        List<SortItemMeta> result = new ArrayList<>();
        for (ISortItem sortItem : sortItems)
        {
            QualifiedName qname = sortItem.getExpression()
                    .getQualifiedColumn();
            if (qname == null)
            {
                return emptyList();
            }
            String column = qname.toDotDelimited();
            if (ESDatasource.INDEX.equals(column))
            {
                result.add(new SortItemMeta(MappedProperty.of("_index", "string"), sortItem.getOrder(), sortItem.getNullOrder()));
                continue;
            }
            // else if (ESOperator.DOCID.equals(column))
            // {
            // // DISABLED: THIS DOES NOT WORK ACROSS ALL ES VERSIONS
            // // Use _uid here since sorting on _id is not supported without extra indexing
            // result.add(new SortItemMeta(MappedProperty.of("_uid", "string"), sortItem.getOrder(), sortItem.getNullOrder()));
            // continue;
            // }

            MappedProperty property = properties.get(column);
            if (property == null)
            {
                return emptyList();
            }

            result.add(new SortItemMeta(property, sortItem.getOrder(), sortItem.getNullOrder()));
        }

        // Consume items from framework
        sortItems.clear();
        return result;
    }

    private IDatasource getTablesDatasource(IQuerySession session, String catalogAlias)
    {
        String endpoint = session.getCatalogProperty(catalogAlias, ENDPOINT_KEY);
        String index = session.getCatalogProperty(catalogAlias, INDEX_KEY);
        Map<String, MappedType> types = getMeta(session, catalogAlias, endpoint, index).getMappedTypes();

        // Collect result and columns
        Set<String> columns = new LinkedHashSet<>();
        List<Map<String, Object>> result = new ArrayList<>(types.size());

        for (Entry<String, MappedType> e : types.entrySet())
        {
            Map<String, Object> meta = new LinkedHashMap<>();
            // Make sure we have a name first
            meta.put(SYS_TABLES_NAME, e.getKey());
            meta.putAll(e.getValue().meta);
            // Make sure name is not overwritten by meta
            meta.put(SYS_TABLES_NAME, e.getKey());
            columns.addAll(meta.keySet());
            result.add(meta);
        }

        Schema schema = new Schema(columns.stream()
                .map(c -> Column.of(c, ResolvedType.of(SYS_TABLES_NAME.equals(c) ? Type.String
                        : Type.Any)))
                .collect(toList()));

        return new IDatasource()
        {
            @Override
            public TupleIterator execute(IExecutionContext context, IDatasourceOptions options)
            {
                return TupleIterator.singleton(new ObjectTupleVector(schema, result.size(), (row, col) ->
                {
                    Map<String, Object> map = result.get(row);
                    Column column = schema.getColumns()
                            .get(col);
                    return map.get(column.getName());
                }));
            }
        };
    }

    private IDatasource getColumnsDatasource(IQuerySession session, String catalogAlias)
    {
        final List<Map<String, Object>> result = new ArrayList<>();
        final String endpoint = session.getCatalogProperty(catalogAlias, ENDPOINT_KEY);
        final String index = session.getCatalogProperty(catalogAlias, INDEX_KEY);
        Map<String, MappedType> types = getMeta(session, catalogAlias, endpoint, index).getMappedTypes();

        Set<String> columns = new LinkedHashSet<>();
        for (Entry<String, MappedType> e : types.entrySet())
        {
            for (MappedProperty prop : e.getValue().properties.values())
            {
                Map<String, Object> meta = new LinkedHashMap<>();
                meta.put(SYS_COLUMNS_TABLE, e.getKey());
                meta.put(SYS_COLUMNS_NAME, prop.name);

                Map<String, Object> tmp = new HashMap<>(prop.meta);
                Object value = tmp.remove(SYS_COLUMNS_TABLE);
                if (value != null)
                {
                    meta.put("_table", value);
                }
                value = tmp.remove(SYS_COLUMNS_NAME);
                if (value != null)
                {
                    meta.put("_name", value);
                }
                meta.putAll(tmp);

                if (prop.indices.size() > 0)
                {
                    meta.put("indices", prop.indices);
                }

                columns.addAll(meta.keySet());
                result.add(meta);
            }
        }

        Comparator<Map<String, Object>> comparator = Comparator.comparing(c -> (String) c.get(SYS_COLUMNS_TABLE));
        comparator = comparator.thenComparing(c -> (String) c.get(SYS_COLUMNS_NAME));
        Collections.sort(result, comparator);

        Schema schema = new Schema(columns.stream()
                .map(c -> Column.of(c, ResolvedType.of(SYS_COLUMNS_TABLE.equalsIgnoreCase(c)
                        || SYS_COLUMNS_NAME.equalsIgnoreCase(c) ? Type.String
                                : Type.Any)))
                .collect(toList()));

        return new IDatasource()
        {
            @Override
            public TupleIterator execute(IExecutionContext context, IDatasourceOptions options)
            {
                return TupleIterator.singleton(new ObjectTupleVector(schema, result.size(), (row, col) ->
                {
                    Map<String, Object> map = result.get(row);
                    Column column = schema.getColumns()
                            .get(col);
                    return map.get(column.getName());
                }));
            }
        };
    }

    private IDatasource getIndicesDatasource(IQuerySession session, String catalogAlias)
    {
        final String endpoint = session.getCatalogProperty(catalogAlias, ENDPOINT_KEY);
        final String index = session.getCatalogProperty(catalogAlias, INDEX_KEY);
        Map<String, MappedType> properties = getMeta(session, catalogAlias, endpoint, index).getMappedTypes();

        String[] columns = new String[] { SYS_INDICES_TABLE, SYS_INDICES_COLUMNS };
        List<Object[]> result = new ArrayList<>(properties.size());

        for (Entry<String, MappedType> e : properties.entrySet())
        {
            List<Index> indices = getIndicesInternal(QualifiedName.of(e.getKey()), e.getValue().properties);

            for (Index ix : indices)
            {
                result.add(new Object[] { e.getKey(), ix.getColumns() });
            }
        }

        Schema schema = new Schema(Arrays.stream(columns)
                .map(c -> Column.of(c, ResolvedType.of(SYS_INDICES_TABLE.equalsIgnoreCase(c) ? Type.String
                        : Type.Any)))
                .collect(toList()));

        return new IDatasource()
        {
            @Override
            public TupleIterator execute(IExecutionContext context, IDatasourceOptions options)
            {
                return TupleIterator.singleton(new ObjectTupleVector(schema, result.size(), (row, col) ->
                {
                    Object[] values = result.get(row);
                    return values[col];
                }));
            }
        };
    }

    private List<Index> getIndicesInternal(QualifiedName table, Map<String, MappedProperty> properties)
    {
        List<Index> result = new ArrayList<>(2 + properties.size());
        // All tables have a doc id index
        result.add(new Index(table, singletonList(ESDatasource.DOCID), Index.ColumnsType.ALL, BATCH_SIZE));

        for (MappedProperty p : properties.values())
        {
            // Nested fields not supported at the moment
            if (p.nestedPath != null)
            {
                continue;
            }

            String field = p.name;
            // Free text mappings cannot be used as index column
            // See if there exists another mapping
            if (p.isFreeTextMapping()
                    && p.getNonFreeTextField() == null)
            {
                continue;
            }

            result.add(new Index(table, singletonList(field), Index.ColumnsType.ALL, BATCH_SIZE));
        }

        return result;
    }
}
