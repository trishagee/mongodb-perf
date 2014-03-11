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

package org.mongodb.perf;

import org.mongodb.Fixture;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.MongoCollection;
import org.mongodb.MongoDatabase;

public class InsertPerformanceTest {
    private static final int NUMBER_OF_OPERATIONS = 10000;
    private static final double NUM_MILLIS_IN_SECOND = 1000;
    private MongoCollection<Document> collection;
    private MongoDatabase database;

    @Before
    public void setUp() {
        database = Fixture.getDefaultDatabase();
        collection = database.getCollection(this.getClass().getName());
        collection.tools().drop();
    }

    @After
    public void tearDown() {
        if (collection != null) {
            collection.tools().drop();
        }
    }

    private void warmup(int numberOfRuns) {
        for (int i = 0; i < numberOfRuns; i++) {
            collection.insert(new Document("name", "String value"));
        }
        System.gc();
        System.gc();
    }

    @Test
    public void shouldInsertString() {
        // Given
        warmup(10000);
        collection.find().remove();

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            collection.insert(new Document("name", "String value"));
        }
        long endTime = System.currentTimeMillis();

        // Then
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d millis\n", timeTaken);
        System.out.printf("Test took: %,.3f seconds\n", timeTaken / NUM_MILLIS_IN_SECOND);
        System.out.printf("%,.0f ops per second%n", (NUM_MILLIS_IN_SECOND / timeTaken) * NUMBER_OF_OPERATIONS);
    }

    @Test
    public void shouldTimeBudget() {
        // to be used in association with tools to output the time at different stages.
        // Given
        warmup(10000);
        collection.find().remove();

        // When
        long startTime = System.nanoTime();
        for (int i = 0; i < 1; i++) {
            collection.insert(new Document("name", "String value"));
        }
        long endTime = System.nanoTime();

        // Then
        System.out.println(startTime);
        System.out.println(endTime);
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d nanos\n", timeTaken);
        //1613000 nanos
    }

    @Test
    public void shouldInsertInt() {
        // Given
        warmup(10000);
        collection.find().remove();

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            collection.insert(new Document("name", 1));
        }
        long endTime = System.currentTimeMillis();

        // Then
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d millis\n", timeTaken);
        System.out.printf("Test took: %,.3f seconds\n", timeTaken / NUM_MILLIS_IN_SECOND);
        System.out.printf("%,.0f ops per second%n", (NUM_MILLIS_IN_SECOND / timeTaken) * NUMBER_OF_OPERATIONS);
    }
}
