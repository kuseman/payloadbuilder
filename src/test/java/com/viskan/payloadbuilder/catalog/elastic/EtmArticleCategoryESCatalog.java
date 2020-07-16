package com.viskan.payloadbuilder.catalog.elastic;

import com.viskan.payloadbuilder.QuerySession;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.parser.QualifiedName;
import com.viskan.payloadbuilder.parser.TableOption;

import static com.viskan.payloadbuilder.utils.CollectionUtils.asSet;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;

/** Catalog for ETM EtmArticle/CategoryV2 synchronizers */
public class EtmArticleCategoryESCatalog extends Catalog
{
    private static final int BATCH_SIZE = 500;
    private static final String DOCTYPE = "EtmArticleV2";
    static final String PROPERTY_PREFIX = EtmArticleCategoryESCatalog.class.getSimpleName();
    static final String NAME = "EtmArticleCategoryES";
    static final String ENDPOINT_KEY = "endpoint";
    static final String INSTANCE_KEY = "instance";
    
    static final List<TypeMapping> TYPE_MAPPINGS = asList(
            new TypeMapping("article", asSet("article"), "EtmArticle_%d", asList("art_id"), DOCTYPE, "article"),
            new TypeMapping("articleName", asSet("articlename", "article_name"), "EtmArticle_%d", asList("art_id"), DOCTYPE, "article"),
            new TypeMapping("articleAttribute", asSet("articleattribute", "article_attribute"), "EtmArticle_%d", asList("art_id"), DOCTYPE, "article"),
            new TypeMapping("articleProperty", asSet("articleproperty", "article_property"), "EtmArticle_%d", asList("art_id"), DOCTYPE, "article"),
            new TypeMapping("articleCategory", asSet("articlecategory","article_category"), "EtmArticle_%d", asList("art_id"), DOCTYPE, "article"),
            new TypeMapping("articleAttributeMedia", asSet("articleattributemedia" ,"article_attribute_media"), "EtmArticle_%d", asList("art_id"), DOCTYPE, "article"),
            new TypeMapping("articlePrentype", asSet("articleprentype" ,"article_prentype"), "EtmArticle_%d", asList("art_id"), DOCTYPE, "article"),

            new TypeMapping("articlePrice", asSet("articleprice"), "EtmArticle_articlePrice_%d_%d", asList("art_id", "country_id"), DOCTYPE, "articlePrice"),
            
            new TypeMapping("attribute1", asSet("attribute1"), "EtmArticle_attribute1_%d", asList("attr1_id"), DOCTYPE, "attribute1"),
            new TypeMapping("attribute2", asSet("attribute2"), "EtmArticle_attribute2_%d", asList("attr2_id"), DOCTYPE, "attribute2"),
            new TypeMapping("attribute3", asSet("attribute3"), "EtmArticle_attribute3_%d", asList("attr3_id"), DOCTYPE, "attribute3"),
            
            new TypeMapping("propertyKey", asSet("propertykey"), "EtmArticle_propertyKey_%d", asList("propertykey_id"), DOCTYPE, "propertyKey"),
            new TypeMapping("propertyValue", asSet("propertyvalue"), "EtmArticle_propertyValue_%d", asList("propertyvalue_id"), DOCTYPE, "propertyValue")
            );
    
    public EtmArticleCategoryESCatalog()
    {
        super(NAME);
    }
    
    @Override
    public Operator getScanOperator(QuerySession session, int nodeId, String catalogAlias, TableAlias tableAlias, List<TableOption> tableOptions)
    {
        TypeMapping mapping = getMapping(tableAlias.getTable().getLast());
        return new EtmArticleCategoryESOperator(mapping, nodeId, catalogAlias, tableAlias, null);
    }
    
    @Override
    public Operator getIndexOperator(QuerySession session, int nodeId, String catalogAlias, TableAlias tableAlias, Index index, List<TableOption> tableOptions)
    {
        TypeMapping mapping = getMapping(tableAlias.getTable().getLast());
        return new EtmArticleCategoryESOperator(mapping, nodeId, catalogAlias, tableAlias, index);
    }
    
    @Override
    public List<Index> getIndices(QuerySession session, String catalogAlias, QualifiedName table)
    {
        TypeMapping mapping = getMapping(table.getLast());
        if (!mapping.docIdPatternColumnNames.isEmpty())
        {
            return singletonList(new Index(table, mapping.docIdPatternColumnNames, BATCH_SIZE));
        }
        
        return emeptyList();
    }
    
    private List<Index> emeptyList()
    {
        return null;
    }

    /** Get type mapping from session with provided table name */
    static final TypeMapping getMapping(String table)
    {
        for (TypeMapping mapping : TYPE_MAPPINGS)
        {
            if (mapping.tableNames.contains(lowerCase(table)))
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
        final String name;
        /** Payloadbuilder table names for this type */
        @JsonProperty
        final Set<String> tableNames;
        /** Pattern for doc id to this type */
        @JsonProperty
        final String docIdPattern;
        /** Column names for the {@link #docIdPattern} components */
        @JsonProperty
        final List<String> docIdPatternColumnNames;
        @JsonProperty
        final String doctype;
        @JsonProperty
        final String subtype;
        
        TypeMapping(
                String name,
                Set<String> tableNames,
                String docIdPattern,
                List<String> docIdPatternColumnNames,
                String doctype,
                String subtype)
        {
            this.name = name;
            this.tableNames = tableNames;
            this.docIdPattern = docIdPattern;
            this.docIdPatternColumnNames = docIdPatternColumnNames;
            this.doctype = doctype;
            this.subtype = subtype;
        }
    }
}
