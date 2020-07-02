package com.viskan.payloadbuilder.provider.elastic;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.parser.QualifiedName;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.MutableInt;

import com.fasterxml.jackson.databind.ObjectMapper;

public class EsCatalog extends Catalog
{
    EsCatalog()
    {
        super("ES");
    }
    
    public EsIndex getIndex()
    {
        
    }
    
    @Override
    public Operator getScanOperator(int nodeId, TableAlias alias)
    {
        if ("source".equals(alias.getTable().toString()))
        {
            String json = "[1001,1003,1004,1005,1006,1008,2002,4040,9001,9002,9004,9005,9006,9007,9010,9018,9019,9026,9027,9028,9029,9030,9040,9044,9046,9072,9075,9077,9078,9079,9089,9094,9106,10250,10273,10311,10314,10315,10358,10374,10446,10484,10485,10486,10487,10488,10489,10490,10491,10492,10493,12021,12022,12033,12034,12039,12040,12046,12049,12339,12343,12347,12349,12350,12353,12365,12366,12374,12376,12450,12452,12458,12469,13345,13350,13351,13352,13368,13373,13382,13383,13437,13557,13574,13575,13621,13622,13674,13676,13677,13678,13679,13683,13687,13688,13690,13691,13696,13697,13698,13707,13708,13710,13711,13712,13713,13859,13860,13898,13935,13944,13963,13964,13965,14067,14070,14071,14072,14073,14079,14132,14142,14180,14215,14259,14265,14266,14267,14269,14270,14271,14304,14330,14335,14336,14341,14496,14516,14517,14566,14585,14645,14646,15446,15449,15458,15459,15460,15463,15466,15495,15496,15497,15498,15499,15500,15509,15511,15513,15520,15546]";
            try
            {
                MutableInt pos = new MutableInt();
                List<Integer> art_ids = new ObjectMapper().readValue(json, List.class);
                return ctx -> art_ids.stream().map(i -> Row.of(alias, pos.getAndIncrement(), new Object[] { i })).iterator();
            }
            catch (IOException e)
            {
            }
            
        }
        
        return new EsOperator(alias, null);
    }
    
    @Override
    public Operator getIndexOperator(int nodeId, TableAlias alias, Index index)
    {
        return new EsOperator(alias, index);
    }
    
    Set<String> ART_ID_INDEX_TABLES = new HashSet<>(asList(
            "article",
            "articleAttribute",
            "articleCategory",
            "articleAttributeMedia",
            "articleProperty"
            ));
    
    @Override
    public List<Index> getIndices(QualifiedName table)
    {
        if (ART_ID_INDEX_TABLES.contains(table.toString()))
        {
            return asList(new Index(table, asList("art_id"), 100));
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
