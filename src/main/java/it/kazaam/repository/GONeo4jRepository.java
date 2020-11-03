package it.kazaam.repository;

import it.kazaam.dto.GOTermNeo4J;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.data.neo4j.annotation.Query;
import org.springframework.data.neo4j.annotation.QueryResult;
import org.springframework.data.neo4j.repository.Neo4jRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface GONeo4jRepository extends Neo4jRepository<GOTermNeo4J, Long> {

    @Query("MATCH (m:GOTerm) RETURN m ORDER BY m ASC")
    Iterable<GOTermNeo4J> findAll();

    @Query("MATCH (m:GOTerm) WHERE m.name =~ ('(?i).*'+$0+'.*') RETURN m")
    GOTermNeo4J findByName(String name);

    @Query("MATCH (m:GOTerm) WHERE m.GOid=$0 RETURN m")
    Optional<GOTermNeo4J> findByGOId(Long goID);

    @Query("MATCH path=(p)-[IS_A*0..]->(:GOTerm{GOid:$0})\n" +
            "WHERE NOT (()-[:IS_A]->(p))\n" +
            "WITH reduce(ids=[], q in nodes(path) | ids+q.GOid) as result\n" +
            "return result")
    List<List<Long>> findAncestors(Long node);

    @Cacheable("successors")
    @Query("MATCH path=(p:GOTerm{GOid:$0})-[:IS_A*]->(f:GOTerm)\n" +
            "WHERE NOT ((f)-[:IS_A]->())\n" +
            "WITH COLLECT(nodes(path)) as nodi\n" +
            "WITH REDUCE(output = [], r IN nodi | output + r) AS flat\n" +
            "WITH REDUCE(output = [], r in flat | output + r.GOid) as flat\n" +
            "UNWIND flat as elements\n" +
            "RETURN distinct elements")
    List<Long> findSuccessors(Long node);

    @Query("MATCH path=(p{GOid:$0})-[r:IS_A*0..]->(f:GOTerm{GOid: $1})\n" +
            "with reduce(ids=[], n in nodes(path)| ids+n.GOid) as res\n" +
            "return res")
    List<List<Long>> findPathsFromNodeToNode(Long start, Long end);

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