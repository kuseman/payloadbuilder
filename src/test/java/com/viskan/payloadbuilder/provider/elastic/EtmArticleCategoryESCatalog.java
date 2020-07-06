package com.viskan.payloadbuilder.provider.elastic;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.parser.QualifiedName;
import com.viskan.payloadbuilder.parser.TableOption;

import static java.util.Arrays.asList;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Catalog for ETM EtmArticle/CategoryV2 synchronizers */
public class EtmArticleCategoryESCatalog extends Catalog
{
    private static final int BATCH_SIZE = 500;
    static final String ARTICLE_DOC_TYPE = "EtmArticleV2";
    static final String NAME = "EtmArticleCategoryES";
    public static String ENDPOINT_KEY = "endpoint";
    public static String INDEX_KEY = "index";
    
    public EtmArticleCategoryESCatalog()
    {
        super(NAME);
    }
    
    @Override
    public Operator getScanOperator(int nodeId, String catalogAlias, TableAlias tableAlias, List<TableOption> tableOptions)
    {
        return new EtmArticleCategoryESOperator(nodeId, catalogAlias, tableAlias, null);
    }
    
    @Override
    public Operator getIndexOperator(int nodeId, String catalogAlias, TableAlias tableAlias, Index index, List<TableOption> tableOptions)
    {
        return new EtmArticleCategoryESOperator(nodeId, catalogAlias, tableAlias, index);
    }
    
   
    /* SETTING */
    
    Set<String> ART_ID_INDEX_TABLES = new HashSet<>(asList(
            "article",
            "articlename",
            "articleattribute",
            "articlecategory",
            "articleattributemedia",
            "articleproperty"
            ));
    
    @Override
    public List<Index> getIndices(QualifiedName table)
    {
        String tbl = lowerCase(table.toString());
        if (ART_ID_INDEX_TABLES.contains(tbl))
        {
            return asList(new Index(table, asList("art_id"), BATCH_SIZE));
        }
        else if ("articleprice".equals(tbl))
        {
            return asList(new Index(table, asList("art_id", "country_id"), 1250));
        }
        else if ("attribute1".equals(tbl))
        {
            return asList(new Index(table, asList("attr1_id"), BATCH_SIZE));
        }
        else if ("attribute2".equals(tbl))
        {
            return asList(new Index(table, asList("attr2_id"), BATCH_SIZE));
        }
        else if ("attribute3".equals(tbl))
        {
            return asList(new Index(table, asList("attr3_id"), BATCH_SIZE));
        }
        else if ("propertykey".equals(tbl))
        {
            return asList(new Index(table, asList("propertykey_id"), BATCH_SIZE));
        }
        else if ("propertyvalue".equals(tbl))
        {
            return asList(new Index(table, asList("propertyvalue_id"), BATCH_SIZE));
        }
        return super.getIndices(table);
    }
    
    /** Class representing a endpoint/index combo */
    protected static class EsIndex
    {
        private final String endpoint;
        private final String index;

        EsIndex(String endpoint, String index)
        {
            this.endpoint = endpoint;
            this.index = index;
        }

        public String getEndpoint()
        {
            return endpoint;
        }
        
        public String getIndex()
        {
            return index;
        }
        
        @Override
        public String toString()
        {
            return index;
        }
    }
}
