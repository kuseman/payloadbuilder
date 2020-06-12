package com.viskan.payloadbuilder.provider.hazelcast;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorContext;

import static com.viskan.payloadbuilder.utils.MapUtils.entry;
import static com.viskan.payloadbuilder.utils.MapUtils.ofEntries;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.ArrayUtils;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

class HzOperator implements Operator
{
//    private static final ObjectMapper MAPPER = new ObjectMapper()
//            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//    private static final TypeReference<List<Object[]>> LIST_OF_OBJECT_ARRAY = new TypeReference<List<Object[]>>()
//    {
//    };
    
//    private static final PoolingHttpClientConnectionManager CONNECTION_MANAGER = new PoolingHttpClientConnectionManager();
//    private static final CloseableHttpClient CLIENT = HttpClientBuilder
//            .create()
//            .setConnectionManager(CONNECTION_MANAGER)
//            .disableCookieManagement()
//            .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
//            .build();

    private final TableAlias tableAlias;
    private final HazelcastInstance hazelcast;

    public HzOperator(HazelcastInstance hazelcast, TableAlias tableAlias)
    {
        this.hazelcast = hazelcast;
        this.tableAlias = tableAlias;
    }

    /* SETTINGS FIELDS */

//    private final Map<String, String> TABLE_TO_SUBTYPE = ofEntries(
//            entry("article", "article"),
//            entry("articleAttribute", "article"));

    private final Map<String, String> TABLE_TO_MAP = ofEntries(
            entry("article", "EtmArticle"),
            entry("articleAttribute", "EtmArticleAttribute"),
            entry("articlePrice", "EtmArticlePrice"),
            entry("articleCategory", "EtmArticleCategory"),
            entry("articleProperty", "EtmArticleProperty"),
            entry("articleAttributeMedia", "EtmArticleAttributeMedia"),
            entry("propertyKey", "EtmPropertyKey"),
            entry("propertyValue", "EtmPropertyValue"),
            entry("attribute1", "EtmAttribute1"),
            entry("attribute2", "EtmAttribute2"),
            entry("attribute3", "EtmAttribute3"));

//        String database = "RamosLager157TestDev".toLowerCase();
//    String database = "RamosOscarJacobsonTestCust".toLowerCase();
//    String database = "RamosSportshopenTestCust".toLowerCase();
    String database = "RamosVnpTestMain".toLowerCase();
    int comp_id = 0;
    String instance = database + (comp_id > 0 ? "_" + comp_id : "");
    String mapPrefix = instance + "_";
    
    /* END SETTINGS FIELD */

    private static class Data
    {
        IMap<Integer, MapRows> mapRows;
        int[] columnIndices;
    }
    
    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        String table = tableAlias.getTable().toString();
        
        Data data = (Data) context.data.computeIfAbsent(table, key -> 
        {
            String mapName = TABLE_TO_MAP.get(table);
            String fullMapName = mapPrefix + mapName;
            String fullMapMetaName = mapPrefix + "MapMeta";
            Map<String, MapMeta> mapMetaByMap = hazelcast.getMap(fullMapMetaName);
            MapMeta mapMeta = mapMetaByMap.get(mapName);
            IMap<Integer, MapRows> mapRows = hazelcast.getMap(fullMapName);
            
            int length = tableAlias.getColumns().length;
            int[] columnIndices = new int[length];
            for (int i=0;i<length;i++)
            {
                int index = ArrayUtils.indexOf(mapMeta.getColumns(), tableAlias.getColumns()[i]);
                columnIndices[i] = index;
            }
            
            Data d = new Data();
            d.mapRows = mapRows;
            d.columnIndices = columnIndices;
            return d; 
        });
        
        Map<Integer, MapRows> map = data.mapRows;
        if (context.getIndex() != null)
        {
            Set<Integer> keys = new HashSet<>();
            while (context.getOuterIndexValues().hasNext())
            {
                keys.add((Integer) context.getOuterIndexValues().next()[0]);
            }
            
            map = data.mapRows.getAll(keys);
        }
        
        
        return getScan(data.columnIndices, map);
    }
   
    private Iterator<Row> getScan(
            int[] columnIndices,
            Map<Integer, MapRows> map)
    {
        final int length = columnIndices.length;
        final Iterator<MapRows> it = map.values().iterator();
        return new Iterator<Row>()
        {
            int pos = 0;
            Row next;
            MapRows current;
            int rowsIndex;
            
            @Override
            public Row next()
            {
                Row r = next;
                next = null;
                return r;
            }
            
            @Override
            public boolean hasNext()
            {
                return setNext();
            }
            
            private boolean setNext()
            {
                while (next == null)
                {
                    if (current == null)
                    {
                        if (!it.hasNext())
                        {
                            return false;
                        }
                        
                        current = it.next();
                        continue;
                    }
                    else if (rowsIndex >= current.getRows().size())
                    {
                        current = null;
                        rowsIndex = 0;
                        continue;
                    }
                    
                    Object[] row = current.getRows().get(rowsIndex);
                    Object[] data = new Object[length];
                    for (int i=0;i<length;i++)
                    {
                        int index = columnIndices[i];
                        data[i] = index != -1 ? row[index] : null;
                    }
                    
                    next = Row.of(tableAlias, pos++, data);
                    
                    rowsIndex++;
                }
                
                return true;
            }
        };
    }
    
    @Override
    public String toString()
    {
        return tableAlias.getTable().toString();
    }
}
