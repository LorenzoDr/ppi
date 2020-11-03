package it.kazaam.repository;

import it.kazaam.models.DisjAncRelationship;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.annotation.QueryResult;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DisjointAncestorsRelationshipRepository extends Neo4jRepository<DisjAncRelationship, Long> {

    @Query("MATCH path=(p)-[:DISJOINT_ANCESTORS]->(:GOTerm{GOid:$0})\n" +
            "WITH [l in relationships(path)| {firstNode: startNode(l).GOid, secondNode: l.secondDisjointAncestor}] as rels\n" +
            "UNWIND rels as result\n" +
            "WITH distinct result as result\n" +
            "return result.firstNode as firstNode, result.secondNode as secondNode")
    Iterable<DisjAncestors> findDisjointAncestors(Long id);
    @QueryResult
    class DisjAncestors {
        Long firstNode;
        Long secondNode;

        public DisjAncestors() {
        }

        public Long getFirstNode() {
            return firstNode;
        }

        public void setFirstNode(Long firstNode) {
            this.firstNode = firstNode;
        }

        public Long getSecondNode() {
            return secondNode;
        }

        public void setSecondNode(Long secondNode) {
            this.secondNode = secondNode;
        }
    }
}