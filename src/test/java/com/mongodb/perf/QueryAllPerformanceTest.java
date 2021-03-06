package com.mongodb.perf;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Fixture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class QueryAllPerformanceTest {
    private static final int NUMBER_OF_OPERATIONS = 20_000;
    private static final double NUM_MILLIS_IN_SECOND = 1000;
    private static final int NUMBER_OF_DOCUMENTS = 1000;

    private DB database;
    private DBCollection collection;
    @SuppressWarnings("PublicField")
    public DBObject[] resultArrayToAvoidOptimization;

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
        DBCursor found = null;
        for (int i = 0; i < numberOfRuns; i++) {
            document.removeField("_id");
            collection.insert(document);
            found = collection.find();
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
    public void testPerformanceOfQueryForAllDocumentsWithSingleStringField() {
        // Given
        warmup(10_000, new BasicDBObject("test", "Document"));
        collection.remove(new BasicDBObject());
        populateCollection(NUMBER_OF_DOCUMENTS, new BasicDBObject("name", "String value"));

        //this array stops the loop from being optimized away by hotspot
        resultArrayToAvoidOptimization = new BasicDBObject[NUMBER_OF_DOCUMENTS];

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            DBCursor cursor = collection.find();
            for (int j = 0; j < NUMBER_OF_DOCUMENTS; j++) {
                resultArrayToAvoidOptimization[j] = cursor.next();
            }
        }
        long endTime = System.currentTimeMillis();

        // Then
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d millis\n", timeTaken);
        System.out.printf("Test took: %,.3f seconds\n", timeTaken / NUM_MILLIS_IN_SECOND);
        double operationsPerSecond = (NUM_MILLIS_IN_SECOND / timeTaken) * NUMBER_OF_OPERATIONS;
        System.out.printf("%.0f ops per second%n", operationsPerSecond);
        System.out.printf("Test,Ops per Second,Time Taken Millis, %n");
        System.out.printf("Query %d Documents,%.0f,%d, %n", NUMBER_OF_DOCUMENTS, operationsPerSecond, timeTaken);
    }

    @Test
    public void testPerformanceOfQueryForAllDocumentsWith100Fields() {
        warmupAndInit();

        //this array stops the loop from being optimized away by hotspot
        resultArrayToAvoidOptimization = new BasicDBObject[NUMBER_OF_DOCUMENTS];

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < 2_000; i++) {
            DBCursor cursor = collection.find();
            for (int j = 0; j < NUMBER_OF_DOCUMENTS; j++) {
                DBObject dbObject = cursor.next();
                resultArrayToAvoidOptimization[j] = dbObject;
            }
        }
        long endTime = System.currentTimeMillis();

        // Then
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d millis\n", timeTaken);
        System.out.printf("Test took: %,.3f seconds\n", timeTaken / NUM_MILLIS_IN_SECOND);
        double operationsPerSecond = (NUM_MILLIS_IN_SECOND / timeTaken) * 2_000;
        System.out.printf("%.0f ops per second%n", operationsPerSecond);
        System.out.printf("Test,Ops per Second,Time Taken Millis, %n");
        System.out.printf("Query %d Documents 100 fields,%.0f,%d, %n", NUMBER_OF_DOCUMENTS, operationsPerSecond, timeTaken);
    }

    private void warmupAndInit() {
        warmup(10_000, new BasicDBObject("test", "Document"));
        collection.remove(new BasicDBObject());
        BasicDBObject document = new BasicDBObject();
        for (int i = 0; i < 100; i++) {
            document.put("field"+i, "value "+i);
        }
        populateCollection(NUMBER_OF_DOCUMENTS, document);
    }

}
