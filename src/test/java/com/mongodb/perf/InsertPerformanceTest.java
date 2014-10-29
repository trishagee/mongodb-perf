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

import com.mongodb.Fixture;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class InsertPerformanceTest {
    private static final int NUMBER_OF_OPERATIONS = 10000;
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

    private void warmup(int numberOfRuns, final Document document) {
        for (int i = 0; i < numberOfRuns; i++) {
            document.remove("_id");
            collection.insertOne(document);
        }
        System.gc();
        System.gc();
    }

    @Test
    public void shouldInsertDocumentWithSingleStringField() {
        // Given
        Document document = new Document("name", "String value");
        warmup(10000, document);
        collection.deleteMany(new Document());

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            document.remove("_id");
            collection.insertOne(document);
        }
        long endTime = System.currentTimeMillis();

        // Then
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d millis\n", timeTaken);
        System.out.printf("Test took: %,.3f seconds\n", timeTaken / NUM_MILLIS_IN_SECOND);
        System.out.printf("%.0f ops per second%n", (NUM_MILLIS_IN_SECOND / timeTaken) * NUMBER_OF_OPERATIONS);
    }

    @Test
    public void shouldInsertDocumentWith100StringValueFields() {
        // Given
        Document document = new Document();
        for (int i = 0; i < 100; i++) {
            document.put("field"+i, "value "+i);
        }
        warmup(10000, document);
        collection.deleteMany(new Document());

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            document.remove("_id");
            collection.insertOne(document);
        }
        long endTime = System.currentTimeMillis();

        // Then
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d millis\n", timeTaken);
        System.out.printf("Test took: %,.3f seconds\n", timeTaken / NUM_MILLIS_IN_SECOND);
        System.out.printf("%.0f ops per second%n", (NUM_MILLIS_IN_SECOND / timeTaken) * NUMBER_OF_OPERATIONS);

    }

    @Test
    public void shouldInsertInt() {
        // Given
        Document document = new Document("name", 1);
        warmup(10000, document);
        collection.deleteMany(new Document());

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            document.remove("_id");
            collection.insertOne(document);
        }
        long endTime = System.currentTimeMillis();

        // Then
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d millis\n", timeTaken);
        System.out.printf("Test took: %,.3f seconds\n", timeTaken / NUM_MILLIS_IN_SECOND);
        System.out.printf("%.0f ops per second%n", (NUM_MILLIS_IN_SECOND / timeTaken) * NUMBER_OF_OPERATIONS);
    }

    @Test
    public void shouldInsertDocumentWith100IntValueFields() {
        // Given
        Document document = new Document();
        for (int i = 0; i < 100; i++) {
            document.put("field"+i, i);
        }
        warmup(10000, document);
        collection.deleteMany(new Document());

        // When
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < NUMBER_OF_OPERATIONS; i++) {
            document.remove("_id");
            collection.insertOne(document);
        }
        long endTime = System.currentTimeMillis();

        // Then
        long timeTaken = endTime - startTime;
        System.out.printf("Time taken: %d millis\n", timeTaken);
        System.out.printf("Test took: %,.3f seconds\n", timeTaken / NUM_MILLIS_IN_SECOND);
        System.out.printf("%.0f ops per second%n", (NUM_MILLIS_IN_SECOND / timeTaken) * NUMBER_OF_OPERATIONS);

    }
}
