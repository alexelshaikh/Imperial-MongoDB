package imperial;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.*;
import static com.mongodb.client.model.Projections.*;

public class FlightsAnalysisRedux {

    private static final String SOURCE = "./src/main/resources/airlinesLines.json";

    private final String databaseName;
    private final String collectionName;

    private MongoClient mongoClient;
    private MongoCollection<Document> mongoCollection;

    public FlightsAnalysisRedux(String databaseName, String collectionName) {
        this.databaseName = databaseName;
        this.collectionName = collectionName;
    }

    public void init() {
        String uri = "mongodb://127.0.0.1:27017";
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(uri))
                .build();

        mongoClient = MongoClients.create(settings);

        mongoClient.getDatabase(databaseName).drop();
        MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);
        mongoCollection = mongoDatabase.getCollection(collectionName, Document.class);
    }

    public void close() {
        if (mongoClient != null) {
            mongoClient.close();
        }
    }

    public void insertData() throws IOException {
        List<Document> batch = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(SOURCE))) {
            String line;
            while ((line = br.readLine()) != null) {
                batch.add(Document.parse(line));
                if (batch.size() >= 1000) {
                    mongoCollection.insertMany(batch);
                    batch.clear();
                }
            }
        }
        if (!batch.isEmpty()) {
            mongoCollection.insertMany(batch);
        }
    }

    public void queryCancelledFlights() {
        long t0 = System.nanoTime();

        Bson filter = and(
                eq("airport.code", "BOS"),
                gt("statistics.flights.cancelled", 100)
        );

        Bson sort = Sorts.orderBy(
                Sorts.ascending("carrier.name"),
                Sorts.descending("statistics.flights.cancelled")
        );

        Bson projection = fields(
                include("carrier.name", "airport.name", "statistics.flights.cancelled"),
                excludeId()
        );

        mongoCollection.find(filter)
                .sort(sort)
                .projection(projection)
                .forEach(System.out::println);

        long elapsedMs = (System.nanoTime() - t0) / 1_000_000;
        System.out.printf("Query Took: %d ms%n", elapsedMs);
    }

    public void queryIndexedCancelledFlights() {
        mongoCollection.createIndex(Indexes.ascending("statistics.flights.cancelled"));
        queryCancelledFlights();
    }

    static void main() {
        FlightsAnalysisRedux flights = new FlightsAnalysisRedux("airlinesDatabase", "airlinesCollection");
        flights.init();
        try {
            flights.insertData();
            flights.queryCancelledFlights();
            flights.queryIndexedCancelledFlights();
        } catch (IOException e) {
            System.out.println(e.getMessage());
        } finally {
            flights.close();
        }
    }
}


