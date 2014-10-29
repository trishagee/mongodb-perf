package com.mongodb.perf.compat;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueryPerformanceTest {
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

    private void warmup(final int numberOfRuns, final DBObject document) {
        DBObject found = null;
        for (int i = 0; i < numberOfRuns; i++) {
            document.removeField("_id");
            collection.insert(document);
            found = collection.find().one();
        }
        System.out.println(found);
        System.gc();
        System.gc();
    }

    private void populateCollection(final int numberOfDocuments, final DBObject document) {
        for (int i = 0; i < numberOfDocuments; i++) {
            document.removeField("_id");
            collection.insert(document);
        }
    }

    @Test
    public void testPerformanceOfQueryForSingleDocumentWithSingleStringField() {
        // Given
        warmup(10_000, new BasicDBObject("test", "Document"));
        collection.remove(new BasicDBObject());
        populateCollection(100, new BasicDBObject("name", "String value"));

        //this array stops the loop from being optimized away by hotspot
        DBObject[] resultArrayToAvoidOptimization = new BasicDBObject[NUMBER_OF_OPERATIONS];

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            resultArrayToAvoidOptimization[i] = collection.find().one();
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

    @Test
    public void testPerformanceOfQueryForSingleDocumentWith100Fields() {
        warmup(10_000, new BasicDBObject("test", "Document"));
        collection.remove(new BasicDBObject());
        BasicDBObject document = new BasicDBObject();
        for (int i = 0; i < 100; i++) {
            document.put("field"+i, "value "+i);
        }
        populateCollection(100, document);

        //this array stops the loop from being optimized away by hotspot
        DBObject[] resultArrayToAvoidOptimization = new BasicDBObject[NUMBER_OF_OPERATIONS];

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            resultArrayToAvoidOptimization[i] = collection.find().one();
        }
        long endTime = System.currentTimeMillis();

        // Then
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d millis\n", timeTaken);
        System.out.printf("Test took: %,.3f seconds\n", timeTaken / NUM_MILLIS_IN_SECOND);
        double operationsPerSecond = (NUM_MILLIS_IN_SECOND / timeTaken) * NUMBER_OF_OPERATIONS;
        System.out.printf("%.0f ops per second%n", operationsPerSecond);
        System.out.printf("Test,Ops per Second,Time Taken Millis, %n");
        System.out.printf("Query Single Document 100 fields,%.0f,%d, %n", operationsPerSecond, timeTaken);
    }

}
