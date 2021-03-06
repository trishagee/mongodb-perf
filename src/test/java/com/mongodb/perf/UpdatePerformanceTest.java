package com.mongodb.perf;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.Fixture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UpdatePerformanceTest {
    private static final int NUMBER_OF_OPERATIONS = 20_000;
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

    private void warmup(final int numberOfRuns, final BasicDBObject document) {
        for (int i = 0; i < numberOfRuns; i++) {
            document.removeField("_id");
            collection.insert(document);
            collection.update(document, new BasicDBObject("$set", new BasicDBObject("new field", "new value")));
        }
        System.gc();
        System.gc();
    }

    private void populateCollection(final int numberOfDocuments, final DBObject document) {
        for (int i = 0; i < numberOfDocuments; i++) {
            document.put("_id", i);
            collection.insert(document);
        }
        System.gc();
        System.gc();
    }

    @Test
    public void testPerformanceOfUpdateForSingleDocumentWithSingleStringField() {
        // Given
        warmup(10_000, new BasicDBObject("test", "Document"));
        collection.remove(new BasicDBObject());
        populateCollection(NUMBER_OF_OPERATIONS, new BasicDBObject("name", "String value"));

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            collection.update(new BasicDBObject("_id", i), new BasicDBObject("$set", new BasicDBObject("new field", "new value")));
        }
        long endTime = System.currentTimeMillis();

        // Then
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d millis\n", timeTaken);
        System.out.printf("Test took: %,.3f seconds\n", timeTaken / NUM_MILLIS_IN_SECOND);
        double operationsPerSecond = (NUM_MILLIS_IN_SECOND / timeTaken) * NUMBER_OF_OPERATIONS;
        System.out.printf("%.0f ops per second%n", operationsPerSecond);
        System.out.printf("Test,Ops per Second,Time Taken Millis, %n");
        System.out.printf("Update Single Document,%.0f,%d, %n", operationsPerSecond, timeTaken);
    }

}
