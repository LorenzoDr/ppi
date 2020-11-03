package it.kazaam.service;

import it.kazaam.repository.AnnotationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class AnnotationService {
    @Autowired
    private AnnotationRepository repo = null;

    public Long countByGOId(Long id) {
        return repo.countByGoID(id);
    }

    public Set<Long> getDistinctGOTermByProtein(String protein){
        return new HashSet<Long>(repo.findDistinctByAspect().stream().filter(doc -> protein.equals(doc.getString("symbol"))).map(doc -> doc.getLong("goID")).collect(Collectors.toList()));
    }
}
