package com.mongodb.perf;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CSharpQueryPerformanceTest {
    private static final double NUM_MILLIS_IN_SECOND = 1000;
    private MongoCollection<Document> collection;
    private MongoDatabase database;

    @Before
    public void setUp() {
        database = com.mongodb.Fixture.getDefaultDatabase();
        collection = database.getCollection(this.getClass().getName());
        collection.dropCollection();
    }
    @After
    public void tearDown() {
        if (collection != null) {
            collection.dropCollection();
        }
        if (database != null) {
            database.dropDatabase();
        }
    }

    private void warmup(int numberOfRuns, final Document document) {
        Document one = null;
        for (int i = 0; i < numberOfRuns; i++) {
            document.remove("_id");
            collection.insertOne(document);
            one = collection.find().first();
        }
        System.out.println(one);
        System.gc();
        System.gc();
    }


    @Test
    public void shouldReadDocuments() {
        // given
        Document document = new Document("name", "String value");
        warmup(10000, document);
        collection.deleteMany(new Document());

        // When
        runBenchmarks(collection, 1, 1, 1);

        int iterations = 1000;
        int[] documentSizes = new int[]{1, 100, 1000};
        for (final int documentSize : documentSizes) {
            System.out.printf("%nBenchmarking documents of size: %d%n", documentSize);
            int[] numberOfDocuments = new int[]{1, 10, 100, 1000};
            for (final int number : numberOfDocuments) {
                runBenchmarks(collection, iterations, number, documentSize);

            }
        }
    }

    private static void runBenchmarks(MongoCollection<Document> collection, int iterations, int numberOfDocuments,
                                      int documentSize) {
        createData(collection, numberOfDocuments, documentSize);
        runBenchmark(collection, iterations, numberOfDocuments);
    }

    private static void createData(MongoCollection<Document> collection, int numberOfDocuments, int documentSize) {
        collection.dropCollection();

        String fillerString = new String(new char[documentSize]).replace("\0", "x");

        for (int id = 0; id < numberOfDocuments; id++) {
            Document document = new Document("_id", id).append("filler", fillerString);
            collection.insertOne(document);
        }
    }

    private static void runBenchmark(MongoCollection<Document> collection, int iterations, int numberOfDocuments) {
        Document document = collection.find().first();

        int totalNumberOfDocumentsRead = 0;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iterations; i++) {
            for (int n = 0; n < numberOfDocuments; n++) {
                document = collection.find().first();
                totalNumberOfDocumentsRead += 1;
            }
        }
        long endTime = System.currentTimeMillis();
        long elapsedMillis = endTime - startTime;
        double documentsPerSecond = totalNumberOfDocumentsRead / (elapsedMillis / NUM_MILLIS_IN_SECOND);
        System.out.println(document);
        System.out.printf("Read %d documents in %d millis, %.1f documents/second%n",
                          totalNumberOfDocumentsRead,
                          elapsedMillis,
                          documentsPerSecond);
    }
}