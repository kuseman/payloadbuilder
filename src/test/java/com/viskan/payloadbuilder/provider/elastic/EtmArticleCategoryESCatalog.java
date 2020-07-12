package com.viskan.payloadbuilder.provider.elastic;

import com.viskan.payloadbuilder.QuerySession;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.parser.QualifiedName;
import com.viskan.payloadbuilder.parser.TableOption;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Catalog for ETM EtmArticle/CategoryV2 synchronizers */
public class EtmArticleCategoryESCatalog extends Catalog
{
    private static final int BATCH_SIZE = 500;
    static final String PROPERTY_PREFIX = EtmArticleCategoryESCatalog.class.getSimpleName();
    static final String TYPEMAPPINGS_KEY = PROPERTY_PREFIX + ".typeMappings"; 
    static final String NAME = "EtmArticleCategoryES";
    public static String ENDPOINT_KEY = "endpoint";
    public static String INSTANCE_KEY = "instance";
    
    public EtmArticleCategoryESCatalog()
    {
        super(NAME);
    }
    
    @Override
    public Operator getScanOperator(QuerySession session, int nodeId, String catalogAlias, TableAlias tableAlias, List<TableOption> tableOptions)
    {
        TypeMapping mapping = getMapping(session, tableAlias.getTable().getLast());
        return new EtmArticleCategoryESOperator(mapping, nodeId, catalogAlias, tableAlias, null);
    }
    
    @Override
    public Operator getIndexOperator(QuerySession session, int nodeId, String catalogAlias, TableAlias tableAlias, Index index, List<TableOption> tableOptions)
    {
        TypeMapping mapping = getMapping(session, tableAlias.getTable().getLast());
        return new EtmArticleCategoryESOperator(mapping, nodeId, catalogAlias, tableAlias, index);
    }
    
   
    /* SETTING */
    
//    Set<String> ART_ID_INDEX_TABLES = new HashSet<>(asList(
//            "article",
//            "articlename",
//            "articleattribute",
//            "articlecategory",
//            "articleattributemedia",
//            "articleproperty"
//            ));
    
    @Override
    public List<Index> getIndices(QuerySession session, String catalogAlias, QualifiedName table)
    {
        TypeMapping mapping = getMapping(session, lowerCase(table.getLast()));
        if (!mapping.docIdPatternColumnNames.isEmpty())
        {
            return singletonList(new Index(table, mapping.docIdPatternColumnNames, BATCH_SIZE));
        }
        
        return emeptyList();
//        List<TypeMapping> typeMappings = session.getProperty(TYPEMAPPINGS_KEY);
//        if (CollectionUtils.isEmpty(typeMappings))
//        {
//            throw new IllegalArgumentException("No type mappings set for " + EtmArticleCategoryESCatalog.class.getSimpleName());
//        }
//        
//        for (TypeMapping mapping : typeMappings)
//        {
//            if (!mapping.docIdPatternColumnNames.isEmpty() && mapping.tableNames.contains(lowerCase(table.getLast())))
//            {
//                return singletonList(new Index(table, mapping.docIdPatternColumnNames, BATCH_SIZE));
//            }
//        }
//        
//        return emptyList();
//        String tbl = lowerCase(table.toString());
//        if (ART_ID_INDEX_TABLES.contains(tbl))
//        {
//            return asList(new Index(table, asList("art_id"), BATCH_SIZE));
//        }
//        else if ("articleprice".equals(tbl))
//        {
//            return asList(new Index(table, asList("art_id", "country_id"), 1250));
//        }
//        else if ("attribute1".equals(tbl))
//        {
//            return asList(new Index(table, asList("attr1_id"), BATCH_SIZE));
//        }
//        else if ("attribute2".equals(tbl))
//        {
//            return asList(new Index(table, asList("attr2_id"), BATCH_SIZE));
//        }
//        else if ("attribute3".equals(tbl))
//        {
//            return asList(new Index(table, asList("attr3_id"), BATCH_SIZE));
//        }
//        else if ("propertykey".equals(tbl))
//        {
//            return asList(new Index(table, asList("propertykey_id"), BATCH_SIZE));
//        }
//        else if ("propertyvalue".equals(tbl))
//        {
//            return asList(new Index(table, asList("propertyvalue_id"), BATCH_SIZE));
//        }
//        return super.getIndices(table);
    }
    
    private List<Index> emeptyList()
    {
        return null;
    }

    /** Get type mapping from session with provided table name */
    static final TypeMapping getMapping(QuerySession session, String table)
    {
        List<TypeMapping> typeMappings = defaultIfNull(session.getProperty(TYPEMAPPINGS_KEY), emptyList());
        for (TypeMapping mapping : typeMappings)
        {
            if (mapping.tableNames.contains(table))
            {
                return mapping;
            }
        }
        
        throw new IllegalArgumentException("No type mappings set for table " + table);
    }
    
    /** Type mapping.
     * <pre>
     * Maps a payloadbuilder table to a EtmArticleCategory type
     * with doc-id pattern, indices, sub types etc. in Elastic
     * </pre>
     *  */
    static class TypeMapping
    {
        /** Type name in ES */
        @JsonProperty
        String name = "";
        /** Payloadbuilder table names for this type */
        @JsonProperty
        Set<String> tableNames = emptySet();
        /** Pattern for doc id to this type */
        @JsonProperty
        String docIdPattern = "";
        /** Column names for the {@link #docIdPattern} components */
        @JsonProperty
        List<String> docIdPatternColumnNames = emptyList();
//        List<Index> indices;
        @JsonProperty
        String doctype = "";
        @JsonProperty
        String subtype = "";
    }
}
