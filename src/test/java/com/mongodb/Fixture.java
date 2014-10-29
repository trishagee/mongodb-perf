/*
 * Copyright (c) 2008 - 2014 MongoDB, Inc.
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

package com.mongodb;


import com.mongodb.client.MongoDatabase;

public final class Fixture {
    public static final String DEFAULT_URI = "mongodb://localhost:27017";
    public static final String MONGODB_URI_SYSTEM_PROPERTY_NAME = "org.mongodb.test.uri";

    private static MongoClientURI mongoClientURI;
    private static MongoClient mongoClient;
    private static MongoDatabase defaultDatabase;

    private Fixture() {
    }

    public static synchronized MongoClient getMongoClient() {
        if (mongoClient == null) {
            MongoClientURI mongoURI = getMongoClientURI();
            mongoClient = new MongoClient(mongoURI);
            Runtime.getRuntime().addShutdownHook(new ShutdownHook());
        }
        return mongoClient;
    }

    public static synchronized MongoDatabase getDefaultDatabase() {
        if (defaultDatabase == null) {
            defaultDatabase = getMongoClient().getDatabase("DriverTest-" + System.nanoTime());
        }
        return defaultDatabase;
    }

    static class ShutdownHook extends Thread {
        @Override
        public void run() {
            if (mongoClient != null) {
                if (defaultDatabase != null) {
                    defaultDatabase.dropDatabase();
                }
                mongoClient.close();
                mongoClient = null;
            }
        }
    }

    public static synchronized MongoClientURI getMongoClientURI() {
        if (mongoClientURI == null) {
            String mongoURIProperty = System.getProperty(MONGODB_URI_SYSTEM_PROPERTY_NAME);
            String mongoURIString = mongoURIProperty == null || mongoURIProperty.isEmpty()
                                    ? DEFAULT_URI : mongoURIProperty;
            mongoClientURI = new MongoClientURI(mongoURIString);
        }
        return mongoClientURI;
    }

}
