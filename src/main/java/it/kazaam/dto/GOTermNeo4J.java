package it.kazaam.dto;

import it.kazaam.models.DisjAncRelationship;
import org.neo4j.ogm.annotation.GeneratedValue;
import org.neo4j.ogm.annotation.Id;
import org.neo4j.ogm.annotation.NodeEntity;
import org.neo4j.ogm.annotation.Relationship;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@NodeEntity(label = "GOTerm")
public class GOTermNeo4J implements Serializable {
//    @Id
//    @GeneratedValue
//    private Long _id;
    @Id
    private Long GOid;
    private String name;
    private String namespace;
    private String definition;
    @Relationship(type = "IS_A", direction = Relationship.INCOMING)
    private Set<GOTermNeo4J> is_a = new HashSet<>();
    @Relationship(type = "DISJOINT_ANCESTORS", direction = Relationship.INCOMING)
    private Set<DisjAncRelationship> disjointAncestors = new HashSet<>();

    public GOTermNeo4J() {
    }

    public GOTermNeo4J(Long id) {
        this.GOid = id;
    }

//    public Long get_id() {
//        return _id;
//    }
//
//    public void set_id(Long _id) {
//        this._id = _id;
//    }

    public Long getGOid() {
        return GOid;
    }

    public void setGOid(Long GOid) {
        this.GOid = GOid;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNamespace() {
        return namespace;
    }

    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    public String getDefinition() {
        return definition;
    }

    public void setDefinition(String definition) {
        this.definition = definition;
    }

    public Set<GOTermNeo4J> getIs_a() {
        return is_a;
    }

    public void setIs_a(Set<GOTermNeo4J> is_a) {
        this.is_a = is_a;
    }

    public Set<DisjAncRelationship> getDisjointAncestors() {
        return disjointAncestors;
    }

    public void setDisjointAncestors(Set<DisjAncRelationship> disjointAncestors) {
        this.disjointAncestors = disjointAncestors;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GOTermNeo4J goTermNeo4J = (GOTermNeo4J) o;

        return GOid.equals(goTermNeo4J.GOid);
    }

    @Override
    public int hashCode() {
        return GOid.hashCode();
    }

}
