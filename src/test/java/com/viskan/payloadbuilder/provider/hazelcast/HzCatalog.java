package com.viskan.payloadbuilder.provider.hazelcast;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;

import static java.util.Arrays.asList;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.mutable.MutableInt;
import org.nustaq.serialization.FSTConfiguration;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.SerializerConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.nio.serialization.ByteArraySerializer;

public class HzCatalog extends Catalog
{
    private HazelcastInstance hazelcast;
    HzCatalog()
    {
        super("HZ");
        initHazelcasr();
    }
    
    private void initHazelcasr()
    {
        ClientConfig config = new ClientConfig();
        config.getGroupConfig().setName("dev");//.setPassword("dev-pass");
        config.getNetworkConfig().addAddress("localhost");
        System.setProperty("hazelcast.jmx", "true");
        config.setInstanceName("EtmDbReplicator");

        config.getProperties().put("hazelcast.jmx", true);
        
        FSTConfiguration conf = FSTConfiguration.createDefaultConfiguration();
        conf.registerClass(MapMeta.class, MapRows.class);
        
        SerializerConfig mapRowsConf = new SerializerConfig();
        mapRowsConf.setTypeClass(MapRows.class);
        mapRowsConf.setImplementation(new MapRowsSerializer(conf));
        SerializerConfig mapMetaConf = new SerializerConfig();
        mapMetaConf.setTypeClass(MapMeta.class);
        mapMetaConf.setImplementation(new MapMetaSerializer(conf));
        
        config.getSerializationConfig().addSerializerConfig(mapRowsConf);
        config.getSerializationConfig().addSerializerConfig(mapMetaConf);
        
        hazelcast = HazelcastClient.newHazelcastClient(config);
    }
    
    @Override
    public Operator getOperator(TableAlias alias)
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
        
        return new HzOperator(hazelcast, alias);
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
        if ("articlePrice".equals(table.toString()))
        {
            return asList(new Index(table, asList("art_id", "sku_id")));
        }
        else if ("attribute1".equals(table.toString()))
        {
            return asList(new Index(table, asList("attr1_id")));
        }
        else if ("attribute2".equals(table.toString()))
        {
            return asList(new Index(table, asList("attr2_id")));
        }
        else if ("attribute3".equals(table.toString()))
        {
            return asList(new Index(table, asList("attr3_id")));
        }
        else if ("propertyKey".equals(table.toString()))
        {
            return asList(new Index(table, asList("propertykey_id")));
        }
        else if ("propertyValue".equals(table.toString()))
        {
            return asList(new Index(table, asList("propertyvalue_id")));
        }
        else if (ART_ID_INDEX_TABLES.contains(table.toString()))
        {
            return asList(new Index(table, asList("art_id")));
        }
        return super.getIndices(table);
    }
    
 
    
    /** Fst serializer for {@link MapRows}. Fast with compression */
    public static class MapRowsSerializer implements ByteArraySerializer<MapRows>
    {
        private static final int TYPE_ID = 123;

        /** FSTConfiguration caches metadata and is being reused for better performance. */
        private final FSTConfiguration conf;

        /**
         * Create a serializer with default FST Configuration {@link FSTConfiguration#createDefaultConfiguration()}
         * 
         * @param typeId
         */
        public MapRowsSerializer()
        {
            this(FSTConfiguration.createDefaultConfiguration());
        }

        public MapRowsSerializer(FSTConfiguration conf)
        {
            this.conf = conf;
        }

        @Override
        public byte[] write(MapRows mapRows) throws IOException
        {
            return conf.asByteArray(mapRows);
        }

        @Override
        public MapRows read(byte[] bytes) throws IOException
        {
            return (MapRows) conf.asObject(bytes);
        }

        @Override
        public int getTypeId()
        {
            return TYPE_ID;
        }

        @Override
        public void destroy()
        {
        }
    }
    
    /** Fst serializer for {@link MapMeta}. Fast with compression */
    public static class MapMetaSerializer implements ByteArraySerializer<MapMeta>
    {
        private static final int TYPE_ID = 124;

        /** FSTConfiguration caches metadata and is being reused for better performance. */
        private final FSTConfiguration conf;

        /**
         * Create a serializer with default FST Configuration {@link FSTConfiguration#createDefaultConfiguration()}
         * 
         * @param typeId
         */
        public MapMetaSerializer()
        {
            this(FSTConfiguration.createDefaultConfiguration());
        }

        public MapMetaSerializer(FSTConfiguration conf)
        {
            this.conf = conf;
        }

        @Override
        public byte[] write(MapMeta mapMeta) throws IOException
        {
            return conf.asByteArray(mapMeta);
        }

        @Override
        public MapMeta read(byte[] bytes) throws IOException
        {
            return (MapMeta) conf.asObject(bytes);
        }

        @Override
        public int getTypeId()
        {
            return TYPE_ID;
        }

        @Override
        public void destroy()
        {
        }
    }
}
