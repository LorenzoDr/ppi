package it.kazaam;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import org.bson.Document;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.mongodb.client.model.Filters.*;

public class AnnotationService {

    private final MongoClient driver;
    private final MongoCollection<Document> annotations;

    public AnnotationService(String uri) {
        driver = new MongoClient(new MongoClientURI(uri));
        annotations = driver.getDatabase("protein-db").getCollection("annotation");
    }

    public Long countByGOId(long id) {
        return annotations.countDocuments(eq("goID", id));
    }

    public Set<Long> getDistinctGOTermByProtein(String protein){
        return new HashSet<>(findDistinctByAspect().stream().filter(doc -> protein.equals(doc.getString("symbol"))).map(doc -> doc.getLong("goID")).collect(Collectors.toList()));
    }

    List<Document> findDistinctByAspect() {
        return annotations.find(eq("aspect", "biological process")).projection(
                and(eq("_id", 0),
                        eq("goID", 1),
                        eq("symbol", 1))).into(new ArrayList<>());
    }

    public void close() {
        driver.close();
    }
}
