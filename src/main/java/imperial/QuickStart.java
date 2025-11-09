package imperial;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import static com.mongodb.client.model.Filters.eq;

public class QuickStart {
    public static void main( String[] args ) {

        // This is the default local connection string! Replace the placeholder with your MongoDB deployment's connection string
        String uri = "mongodb://127.0.0.1:27017";

        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase("testdb");

            // Collection handler. If the collection is not yet created but data is inserted, the collection is created automatically
            MongoCollection<Document> collection = database.getCollection("testcollection");

            Document doc = collection.find(eq("lecturedate", "11.11.2025")).first();
            if (doc != null) {
                System.out.println(doc.toJson());
            } else {
                System.out.println("No matching documents found.");
            }
        }
    }
}
