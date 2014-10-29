package com.mongodb.perf;

import com.mongodb.Fixture;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueryPerformanceTest {
    private static final int NUMBER_OF_OPERATIONS = 20_000;
    private static final double NUM_MILLIS_IN_SECOND = 1000;
    private MongoCollection<Document> collection;
    private MongoDatabase database;

    @Before
    public void setUp() {
        database = Fixture.getDefaultDatabase();
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

    private void warmup(final int numberOfRuns, final Document document) {
        Document found = null;
        for (int i = 0; i < numberOfRuns; i++) {
            document.remove("_id");
            collection.insertOne(document);
            found = collection.find().first();
        }
        System.out.println(found);
        System.gc();
        System.gc();
    }

    private void populateCollection(final int numberOfDocuments, final Document document) {
        for (int i = 0; i < numberOfDocuments; i++) {
            document.remove("_id");
            collection.insertOne(document);
        }
    }

    @Test
    public void testPerformanceOfQueryForSingleDocumentWithSingleStringField() {
        // Given
        warmup(10_000, new Document("test", "Document"));
        collection.deleteMany(new Document());
        populateCollection(100, new Document("name", "String value"));

        //test with array
        Document[] resultArrayToAvoidOptimization = new Document[NUMBER_OF_OPERATIONS];

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            resultArrayToAvoidOptimization[i] = collection.find().first();
        }
        long endTime = System.currentTimeMillis();

        // Then
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d millis\n", timeTaken);
        System.out.printf("Test took: %,.3f seconds\n", timeTaken / NUM_MILLIS_IN_SECOND);
        double operationsPerSecond = (NUM_MILLIS_IN_SECOND / timeTaken) * NUMBER_OF_OPERATIONS;
        System.out.printf("%.0f ops per second%n", operationsPerSecond);
        System.out.printf("Test,Ops per Second,Time Taken Millis, %n");
        System.out.printf("Query Single Document,%.0f,%d, %n", operationsPerSecond, timeTaken);
    }

}
