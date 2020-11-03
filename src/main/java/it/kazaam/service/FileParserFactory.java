/**
 *
 */
package it.kazaam.service;

import it.kazaam.dto.GOTermMongo;
import it.kazaam.dto.GOTermNeo4J;
import it.kazaam.models.Annotation;
import it.kazaam.models.GOTerm;
import it.kazaam.repository.AnnotationRepository;
import it.kazaam.repository.GONeo4jRepository;
import it.kazaam.repository.GOMongoRepository;
import it.kazaam.utility.ObjectMapperUtils;
import org.apache.commons.io.FileUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;
import java.util.stream.Stream;

/**
 * @author salvatore
 *
 */

@Service
public class FileParserFactory {
    private final Logger logger = LoggerFactory.getLogger(FileParserFactory.class);
    @Autowired
    private JavaSparkContext jsc;
    private final String RESOURCE_PATH = "src/main/resources/gene-ontologies/";

    private static AnnotationRepository annotationRepository;
    private static GOMongoRepository goMongoRepository;
    private static GONeo4jRepository goNeo4jRepository;

    @Autowired
    public FileParserFactory(AnnotationRepository annotationRepository_, GOMongoRepository goMongoRepository_, GONeo4jRepository goNeo4jRepository_) {
        annotationRepository = annotationRepository_;
        goMongoRepository = goMongoRepository_;
        goNeo4jRepository = goNeo4jRepository_;
    }

    public void parse() {
        JavaRDD<GOTerm> goTerms = getRDDTerms();
//        goTerms = jsc.parallelize(goTerms.take(10));
        JavaRDD<Annotation> annotations = getAnnotations();
        logger.info(String.format("Saving %d GO Terms", goTerms.count()));
        goTerms.foreach(goTerm -> {
            GOTermMongo goTermMongo = ObjectMapperUtils.map(goTerm, GOTermMongo.class);
            saveGoTerm(goTermMongo);
            GOTermNeo4J goTermNeo4J = ObjectMapperUtils.map(goTerm, GOTermNeo4J.class);
            goTermNeo4J.getIs_a().addAll(ObjectMapperUtils.mapAll(goTerm.getIs_a(), GOTermNeo4J.class));
            saveGoTermNeo4J(goTermNeo4J);
        });
        logger.info(String.format("Saved %d GO Terms", goMongoRepository.findAll().size()));
        logger.info(String.format("Saved %d GO Terms", Stream.of(goNeo4jRepository.findAll().iterator()).count()));
        logger.info(String.format("Saving %d Annotations", annotations.count()));
        annotations.foreach(annotation -> saveAnnotation(annotation));
        logger.info(String.format("Saved %d Annotations", annotationRepository.findAll().size()));
    }

    private static void saveGoTermNeo4J(GOTermNeo4J goTermNeo4J) {
        Iterable<GOTermNeo4J> results = goNeo4jRepository.findAll();
        GOTermNeo4J saved = goNeo4jRepository.save(goTermNeo4J);
    }

    private static void saveGoTerm(GOTermMongo goTermMongo) {
        GOTermMongo goTerm_Mongo_db = goMongoRepository.findById(goTermMongo.getId()).orElse(null);
        if (goTerm_Mongo_db != null) {
            if (goTerm_Mongo_db.getName() == null && goTermMongo.getName() != null) {
                goTerm_Mongo_db.setName(goTermMongo.getName());
            }
            if (goTerm_Mongo_db.getNamespace() == null && goTermMongo.getNamespace() != null) {
                goTerm_Mongo_db.setNamespace(goTermMongo.getNamespace());
            }
            if (goTerm_Mongo_db.getDefinition() == null && goTermMongo.getDefinition() != null) {
                goTerm_Mongo_db.setDefinition(goTermMongo.getDefinition());
            }
            goTerm_Mongo_db.getIs_a().addAll(goTermMongo.getIs_a());
            goMongoRepository.save(goTerm_Mongo_db);
        } else goMongoRepository.save(goTermMongo);
    }

    private static void saveAnnotation(Annotation annotation) {
        annotationRepository.save(annotation);
    }

    public JavaRDD<GOTerm> getRDDTerms() {
        File file = new File(RESOURCE_PATH + "go.obo");
        JavaRDD<String> termsRDD = null;
        try {
            termsRDD = jsc.parallelize(Arrays.asList(FileUtils.readFileToString(file).split("\\n\\n")));
        } catch (IOException e) {
            e.printStackTrace();
        }
        JavaRDD<String> filteredTermsRDD = termsRDD.filter(block -> !block.startsWith("!"));
        return filteredTermsRDD.map(GOTerm::convertOBOBlock).filter(Objects::nonNull);
    }

    public JavaRDD<Annotation> getAnnotations() {
        JavaRDD<String> fileRDD = jsc.textFile(RESOURCE_PATH + "goa_human.gaf");
        JavaRDD<String> annotations = fileRDD.filter(line -> !line.startsWith("!"));
        return annotations.map(line -> new Annotation(line.split("\t")));
    }
}
