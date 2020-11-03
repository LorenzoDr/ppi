package it.kazaam.service;

import it.kazaam.repository.DisjointAncestorsRelationshipRepository;
import it.kazaam.repository.GONeo4jRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Service
public class GONeo4jService {
    private final Logger logger = LoggerFactory.getLogger(GONeo4jService.class);
    @Autowired
    private GONeo4jRepository repository = null;
    @Autowired
    private DisjointAncestorsRelationshipRepository disjRepository = null;

    public Set<Long> getAncestors(Long node) {
        Set<Long> ancestors = new HashSet<>();
        ancestors.add(node);
        for (List<Long> path : repository.findAncestors(node)) {
            for (Long goTerm : path) {
                ancestors.add(goTerm);
            }
        }
        return ancestors;
    }
}
