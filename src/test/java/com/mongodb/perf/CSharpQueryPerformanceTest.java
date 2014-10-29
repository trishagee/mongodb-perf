package com.mongodb.perf;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Fixture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CSharpQueryPerformanceTest {
    private static final int NUMBER_OF_OPERATIONS = 10000;
    private static final double NUM_MILLIS_IN_SECOND = 1000;

    private DB database;
    private DBCollection collection;

    @Before
    public void setUp() {
        database = Fixture.getDefaultDatabase();
        collection = database.getCollection(this.getClass().getName());
        collection.drop();
    }

    @After
    public void tearDown() {
        if (collection != null) {
            collection.drop();
        }
        if (database != null) {
            database.dropDatabase();
        }
    }

    private void warmup(int numberOfRuns, final DBObject document) {
        DBObject one = null;
        for (int i = 0; i < numberOfRuns; i++) {
            document.removeField("_id");
            collection.insert(document);
            one = collection.find().one();
        }
        System.out.println(one);
        System.gc();
        System.gc();
    }


    @Test
    public void shouldReadDocuments() {
        // given
        DBObject document = new BasicDBObject("name", "String value");
        warmup(10000, document);
        collection.remove(new BasicDBObject());

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

    private static void runBenchmarks(DBCollection collection, int iterations, int numberOfDocuments,
                                      int documentSize) {
        createData(collection, numberOfDocuments, documentSize);
        runBenchmark(collection, iterations, numberOfDocuments);
    }

    private static void createData(DBCollection collection, int numberOfDocuments, int documentSize) {
        collection.drop();

        String fillerString = new String(new char[documentSize]).replace("\0", "x");

        for (int id = 0; id < numberOfDocuments; id++) {
            DBObject document = new BasicDBObject("_id", id).append("filler", fillerString);
            collection.insert(document);
        }
    }

    private static void runBenchmark(DBCollection collection, int iterations, int numberOfDocuments) {
        DBObject document = collection.find().one();

        int totalNumberOfDocumentsRead = 0;
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < iterations; i++) {
            for (int n = 0; n < numberOfDocuments; n++) {
                document = collection.find().one();
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