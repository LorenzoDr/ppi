package it.kazaam.repository;

import it.kazaam.dto.GOTermMongo;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;

import java.util.List;

public interface GOMongoRepository extends MongoRepository<GOTermMongo, Long> {

    @Query("{'GOid': ?0}")
    GOTermMongo findByGOId(Long value);

    @Query("{'namespace': ?0}")
    List<GOTermMongo> findAllInAspect(String value);

}
