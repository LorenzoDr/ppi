package it.kazaam;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.bson.Document;

import java.util.HashSet;
import java.util.Set;

public class AnnotationService {

    public final JavaRDD<Document> annotations;

    public AnnotationService(SparkContext spark) {
        annotations = MongoSpark.load(JavaSparkContext.fromSparkContext(spark)).cache();
    }

    public Long countByGOId(long id) {
        return annotations.aggregate(0L, (c, doc) -> c + (doc.getLong("goID") == id ? 1 : 0), Long::sum);
    }

    public Set<Long> getDistinctGOTermByProtein(String protein){
        return new HashSet<>(annotations.filter(doc -> doc.getString("aspect").equals("biological process") && doc.getString("symbol").
                equals(protein)).map(doc -> doc.getLong("goID")).collect());
    }
}
