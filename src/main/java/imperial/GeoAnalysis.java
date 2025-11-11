package imperial;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.*;
import com.mongodb.client.model.GeoNearOptions;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.geojson.Point;
import com.mongodb.client.model.geojson.Position;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.mongodb.client.model.Aggregates.*;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.ascending;

public class GeoAnalysis {

    private static final String SOURCE = "./src/main/resources/marburg.geojson";

    private final String databaseName;
    private final String collectionName;

    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;
    private MongoCollection<Document> mongoCollection;

    public GeoAnalysis(String databaseName, String collectionName) {
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
        mongoDatabase = mongoClient.getDatabase(databaseName);
        mongoCollection = mongoDatabase.getCollection(collectionName, Document.class);
    }

    public void close() {
        mongoClient.close();
    }

    public void insertData() throws FileNotFoundException {
        JSONTokener tokener = new JSONTokener(new FileReader(SOURCE));
        JSONArray array = new JSONArray(tokener);

        List<Document> objectList = new ArrayList<>();
        for (int i = 0; i < array.length(); i++) {
            JSONObject jsonObject = array.getJSONObject(i);
            if (jsonObject.getJSONObject("geometry").getString("type").equals("Point")) {
                Document parsed = Document.parse(jsonObject.toString());
                objectList.add(parsed);
            }
        }
        mongoCollection.insertMany(objectList);
        mongoCollection.createIndex(Indexes.geo2dsphere("geometry"));
    }


    public List<Double> getFB() {
        Document doc = mongoCollection.find(eq("properties.name", "Fachbereich Mathematik und Informatik"))
                .projection(fields(include("geometry.coordinates"), excludeId()))
                .first();

        if (doc == null) {
            System.err.println("Fachbereich Mathematik und Informatik not found!");
            return null;
        }

        List<Double> coords = doc.get("geometry", Document.class)
                .getList("coordinates", Double.class);

        System.out.println("Fachbereich Mathematik und Informatik coordinates: " + coords);

        return coords;
    }

    public void findRestaurants() {
        List<Double> loc = getFB();
        if (loc == null) {
            System.err.println("Cannot find restaurants — reference location not found.");
            return;
        }

        Document restaurantFilter = new Document("properties.amenity", "restaurant");

        Point nearPoint = new Point(new Position(loc.get(0), loc.get(1)));

        GeoNearOptions geoNearOptions = GeoNearOptions.geoNearOptions()
                .query(restaurantFilter)
                .spherical();

        List<Bson> pipeline = Arrays.asList(
                geoNear(nearPoint, "dis", geoNearOptions),
                sort(ascending("dis")),
                limit(10)
        );

        AggregateIterable<Document> result = mongoCollection.aggregate(pipeline);

        result.forEach(doc -> {
            Document properties = doc.get("properties", Document.class);
            Object name = properties != null ? properties.get("name") : null;
            Object dist = doc.get("dis");
            System.out.println(name + ", dist=" + dist + " meters");
        });
    }

    static void main() {
        GeoAnalysis geoAnalysis = new GeoAnalysis("marburgDatabase", "marburgCollection");
        System.out.println("initialising...");
        geoAnalysis.init();
        try {
            System.out.println("Inserting data...");
            geoAnalysis.insertData();

            System.out.println("Finding the nearby Restaurants based on Fachbereich Mathematik und Informatik location...\n");
            geoAnalysis.findRestaurants();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            geoAnalysis.close();
        }
    }
}
