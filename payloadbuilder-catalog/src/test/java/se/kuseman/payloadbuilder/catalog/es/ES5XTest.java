package se.kuseman.payloadbuilder.catalog.es;

import org.junit.AfterClass;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import se.kuseman.payloadbuilder.catalog.es.ElasticsearchMeta.Version;

/** Test with elastic search 5X */
public class ES5XTest extends BaseESTest
{
    private static final String TYPE = "testType";

    public ES5XTest()
    {
        super(ES.ES_ENDPOINT, TYPE, Version._5X);
    }

    @AfterClass
    public static void tearDownClass()
    {
        ES.stop();
    }

    static class ES
    {
        private static final String IMAGE_NAME = "docker.elastic.co/elasticsearch/elasticsearch:5.6.16";
        private static final ElasticsearchContainer ES_CONTAINER = new ElasticsearchContainer(IMAGE_NAME);

        static final String ES_ENDPOINT;

        static
        {
            ES_CONTAINER.setWaitStrategy(new HostPortWaitStrategy());
            ES_CONTAINER.withEnv("xpack.security.enabled", "false")
                    .withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx512m -XX:MaxDirectMemorySize=536870912");
            ES_CONTAINER.start();

            ES_ENDPOINT = "http://" + ES_CONTAINER.getHttpHostAddress();

            Runtime.getRuntime()
                    .addShutdownHook(new Thread(() -> ES_CONTAINER.stop()));
        }

        static void stop()
        {
            if (ES_CONTAINER != null)
            {
                ES_CONTAINER.stop();
            }
        }
    }
}
