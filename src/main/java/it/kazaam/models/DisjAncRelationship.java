package it.kazaam.models;

import it.kazaam.dto.GOTermNeo4J;
import org.neo4j.ogm.annotation.*;

import java.io.Serializable;
import java.util.Objects;

@RelationshipEntity(type = "DISJOINT_ANCESTORS")
public class DisjAncRelationship implements Serializable {
    @Id
    @GeneratedValue
    Long id;
    @StartNode
    private GOTermNeo4J firstDisjointAncestor;
    @Property
    private Long secondDisjointAncestor;    // Id del secondo GOTerm
    @EndNode
    private GOTermNeo4J goTerm;

    public DisjAncRelationship() {
    }

    public DisjAncRelationship(GOTermNeo4J firstDisjointAncestor, GOTermNeo4J secondDisjointAncestor, GOTermNeo4J goTerm) {
        this.secondDisjointAncestor = secondDisjointAncestor.getGOid();
        this.firstDisjointAncestor = firstDisjointAncestor;
        this.goTerm = goTerm;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }


    public void setSecondDisjointAncestor(Long secondDisjointAncestor) {
        this.secondDisjointAncestor = secondDisjointAncestor;
    }

    public Long getSecondDisjointAncestor() {
        return secondDisjointAncestor;
    }

    public GOTermNeo4J getFirstDisjointAncestor() {
        return firstDisjointAncestor;
    }

    public void setFirstDisjointAncestor(GOTermNeo4J firstDisjointAncestor) {
        this.firstDisjointAncestor = firstDisjointAncestor;
    }

    public GOTermNeo4J getGoTerm() {
        return goTerm;
    }

    public void setGoTerm(GOTermNeo4J goTerm) {
        this.goTerm = goTerm;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DisjAncRelationship that = (DisjAncRelationship) o;
        return id.equals(that.id) &&
                firstDisjointAncestor.equals(that.firstDisjointAncestor) &&
                secondDisjointAncestor.equals(that.secondDisjointAncestor) &&
                goTerm.equals(that.goTerm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, firstDisjointAncestor, secondDisjointAncestor, goTerm);
    }
}
