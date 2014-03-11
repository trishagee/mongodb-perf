import org.junit.Test;
import org.mongodb.Document;
import org.mongodb.MongoClient;
import org.mongodb.MongoClients;
import org.mongodb.MongoCollection;
import org.mongodb.connection.ServerAddress;

import javax.print.Doc;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import static java.lang.String.format;
import static java.lang.System.out;

/** Jeff's test **/
public class InsertBatchPerformanceTest {
    @Test
    public void should() throws UnknownHostException {
        MongoClient mongoClient = MongoClients.create(new ServerAddress());
        try {
            MongoCollection<Document> coll = mongoClient.getDatabase("test").getCollection("test");

            int count = 200000;
            int documentSize = 400;

            System.out.println("Warming up");
            System.out.println();
            System.out.println();

            // warm up mongo
            benchmark(coll, createDocumentList(documentSize, 1000), count / 1000);

            // warmup Java
            for (int i = 0; i < 100; i++) {
                benchmark(coll, createDocumentList(5, 1000), 1);
            }

            System.out.println("Starting benchmark");
            System.out.println();
            System.out.println();

            benchmark(coll, createDocumentList(documentSize, 1), count);
            benchmark(coll, createDocumentList(documentSize, 10), count / 10);
            benchmark(coll, createDocumentList(documentSize, 1000), count / 1000);
            benchmark(coll, createDocumentList(documentSize, 10000), count / 10000);
        } finally {
            mongoClient.close();
        }

    }

    public static void benchmark(final MongoCollection<Document> collection, final List<Document> documents, final int batchCount) {
        out.println(format("Benchmarking documentSize=%d batchSize=%d", ((String) documents.get(0).get("filler")).length(),
                           documents.size()));

        collection.tools().drop();

        long startTime = System.currentTimeMillis();

        for (int i = 0; i < batchCount; i++) {
            removeDocumentIds(documents);
            collection.insert(documents);
        }

        long elapsed = System.currentTimeMillis() - startTime;
        long count = collection.find().count();
        out.println("Count: " + count);
        out.println(format("Duration = %d Speed=%2$,.2f/second", elapsed, count / (elapsed / 1000.0)));
        out.println();

    }

    private static void removeDocumentIds(final List<Document> documents) {
        for (final Document cur : documents) {
            cur.remove("_id");
        }
    }

    private static List<Document> createDocumentList(final int documentSize, final int batchCount) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < documentSize; i++) {
            builder.append('x');
        }
        String filler = builder.toString();

        List <Document> documents = new ArrayList<Document>(batchCount);
        for (int i = 0; i < batchCount; i++) {
            documents.add(new Document("filler", filler));
        }
        return documents;
    }
}