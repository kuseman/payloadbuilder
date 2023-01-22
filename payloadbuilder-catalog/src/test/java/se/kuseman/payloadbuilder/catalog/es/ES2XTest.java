package se.kuseman.payloadbuilder.catalog.es;

import org.junit.AfterClass;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import se.kuseman.payloadbuilder.catalog.es.ElasticsearchMeta.Version;

/** Test with elastic search 2X */
public class ES2XTest extends BaseESTest
{
    private static final String TYPE = "testType";

    public ES2XTest()
    {
        super(ES.ES_ENDPOINT, TYPE, Version._2X);
    }

    @AfterClass
    public static void tearDownClass()
    {
        ES.stop();
    }

    static class ES
    {
        private static final int PORT = 9200;
        private static final String IMAGE_NAME = "elasticsearch:2.4.6";
        private static final GenericContainer<?> ES_CONTAINER = new GenericContainer<>(DockerImageName.parse(IMAGE_NAME)).withExposedPorts(PORT);

        static final String ES_ENDPOINT;

        static
        {
            ES_CONTAINER.setWaitStrategy(new HostPortWaitStrategy());
            ES_CONTAINER.withEnv("xpack.security.enabled", "false")
                    .withEnv("ES_JAVA_OPTS", "-Xms256m -Xmx512m -XX:MaxDirectMemorySize=536870912");
            ES_CONTAINER.start();

            ES_ENDPOINT = "http://" + ES_CONTAINER.getHost() + ":" + ES_CONTAINER.getMappedPort(PORT);

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
