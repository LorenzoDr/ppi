package it.kazaam.repository;

import it.kazaam.models.Annotation;
import it.kazaam.dto.GOTermMongo;
import org.bson.Document;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import java.util.List;

public interface AnnotationRepository extends MongoRepository<Annotation, Long> {

    @Query("{'annotationID': ?0}")
    Annotation findByAnnotationId(String value);

    @Query("{'annotationID': ?0, 'db': ?1, 'goID': ?2, 'aspect': ?3}")
    Annotation findDuplicates(String annotationId, String db, Long goID, String aspect);

    @Query("{'aspect': ?0}")
    List<GOTermMongo> findAllInAspect(String value);

    @Query(value = "{'goID':?0}", count = true)
    Long countByGoID(Long id);

    @Query(value="{'aspect':'biological process'}", fields="{ _id: 0,goID : 1, symbol : 1}")
    List<Document> findDistinctByAspect();
}
