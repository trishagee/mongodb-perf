/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.perf.compat;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InsertPerformanceTest {
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

    private void warmup(final int numberOfRuns, final DBObject document) {
        for (int i = 0; i < numberOfRuns; i++) {
            document.removeField("_id");
            collection.insert(document);
        }
        System.gc();
        System.gc();
    }

    @Test
    public void shouldInsertString() {
        // Given
        DBObject document = new BasicDBObject("name", "String value");
        warmup(10000, document);
        collection.remove(new BasicDBObject());

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            document.removeField("_id");
            collection.insert(document);
        }
        long endTime = System.currentTimeMillis();

        // Then
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d millis\n", timeTaken);
        System.out.printf("Test took: %,.3f seconds\n", timeTaken / NUM_MILLIS_IN_SECOND);
        double operationsPerSecond = (NUM_MILLIS_IN_SECOND / timeTaken) * NUMBER_OF_OPERATIONS;
        System.out.printf("%.0f ops per second%n", operationsPerSecond);
        System.out.printf("Test,Ops per Second,Time Taken Millis, %n");
        System.out.printf("Single String field,%.0f,%d, %n", operationsPerSecond, timeTaken);
    }

    @Test
    public void shouldInsertDocumentWith100StringValueFields() {
        // Given
        DBObject document = new BasicDBObject();
        for (int i = 0; i < 100; i++) {
            document.put("field" + i, "value " + i);
        }
        warmup(10000, document);
        collection.remove(new BasicDBObject());

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            document.removeField("_id");
            collection.insert(document);
        }
        long endTime = System.currentTimeMillis();

        // Then
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d millis\n", timeTaken);
        System.out.printf("Test took: %,.3f seconds\n", timeTaken / NUM_MILLIS_IN_SECOND);
        double operationsPerSecond = (NUM_MILLIS_IN_SECOND / timeTaken) * NUMBER_OF_OPERATIONS;
        System.out.printf("%.0f ops per second%n", operationsPerSecond);
        System.out.printf("Test,Ops per Second,Time Taken Millis, %n");
        System.out.printf("100 String fields,%.0f,%d, %n", operationsPerSecond, timeTaken);
    }

    @Test
    public void shouldInsertInt() {
        // Given
        DBObject document = new BasicDBObject("name", 1);
        warmup(10000, document);
        collection.remove(new BasicDBObject());

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            document.removeField("_id");
            collection.insert(document);
        }
        long endTime = System.currentTimeMillis();

        // Then
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d millis\n", timeTaken);
        System.out.printf("Test took: %,.3f seconds\n", timeTaken / NUM_MILLIS_IN_SECOND);
        double operationsPerSecond = (NUM_MILLIS_IN_SECOND / timeTaken) * NUMBER_OF_OPERATIONS;
        System.out.printf("%.0f ops per second%n", operationsPerSecond);
        System.out.printf("Test,Ops per Second,Time Taken Millis, %n");
        System.out.printf("Single int field,%.0f,%d, %n", operationsPerSecond, timeTaken);
    }

    @Test
    public void shouldInsertDocumentWith100IntValueFields() {
        // Given
        DBObject document = new BasicDBObject();
        for (int i = 0; i < 100; i++) {
            document.put("field" + i, i);
        }
        warmup(10000, document);
        collection.remove(new BasicDBObject());

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            document.removeField("_id");
            collection.insert(document);
        }
        long endTime = System.currentTimeMillis();

        // Then
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d millis\n", timeTaken);
        System.out.printf("Test took: %,.3f seconds\n", timeTaken / NUM_MILLIS_IN_SECOND);
        double operationsPerSecond = (NUM_MILLIS_IN_SECOND / timeTaken) * NUMBER_OF_OPERATIONS;
        System.out.printf("%.0f ops per second%n", operationsPerSecond);
        System.out.printf("Test,Ops per Second, Time Taken Millis, %n");
        System.out.printf("100 Int Fields,%.0f,%d, %n", operationsPerSecond, timeTaken);
    }
}
