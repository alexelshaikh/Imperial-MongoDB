package imperial;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import java.util.Arrays;
import java.util.List;

public class HelloMongoDB {

    static void main() {
        // 1. Connect to local MongoDB
        String uri = "mongodb://127.0.0.1:27017";
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri))
                .build();

        try (MongoClient mongoClient = MongoClients.create(settings)) {
            // 2. Create or get a database
            MongoDatabase database = mongoClient.getDatabase("helloDatabase");

            // 3. Create or get a collection
            MongoCollection<Document> collection = database.getCollection("groupMembers", Document.class);

            // Clear old data (optional, for clean reruns)
            collection.drop();

            // 4. Insert group members
            List<Document> members = Arrays.asList(
                    new Document("name", "Alice"),
                    new Document("name", "Bob"),
                    new Document("name", "Charlie")
            );

            collection.insertMany(members);

            // 5. Retrieve and print all documents
            System.out.println("Group members in the collection:");
            collection.find().forEach(doc -> System.out.println(doc.toJson()));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
