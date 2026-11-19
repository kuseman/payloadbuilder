package se.kuseman.payloadbuilder.catalog.mongodb;

import org.junit.jupiter.api.AfterAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.DockerImageName;

/** Test with MongoDB 7.X. */
class Mongo7XTest extends BaseMongoTest
{
    Mongo7XTest()
    {
        super(Mongo.CONNECTION_STRING);
    }

    @AfterAll
    static void tearDownClass()
    {
        Mongo.stop();
    }

    static class Mongo
    {
        private static final int PORT = 27017;
        private static final String IMAGE_NAME = "mongo:7.0.39";
        @SuppressWarnings("resource")
        private static final GenericContainer<?> MONGO_CONTAINER = new GenericContainer<>(DockerImageName.parse(IMAGE_NAME)).withExposedPorts(PORT);

        static final String CONNECTION_STRING;

        static
        {
            MONGO_CONTAINER.setWaitStrategy(new LogMessageWaitStrategy().withRegEx(".*Waiting for connections.*"));
            MONGO_CONTAINER.start();

            CONNECTION_STRING = "mongodb://" + MONGO_CONTAINER.getHost() + ":" + MONGO_CONTAINER.getMappedPort(PORT);

            Runtime.getRuntime()
                    .addShutdownHook(new Thread(() -> MONGO_CONTAINER.stop()));
        }

        static void stop()
        {
            if (MONGO_CONTAINER != null)
            {
                MONGO_CONTAINER.stop();
            }
        }
    }
}
