package it.kazaam.dto;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.DBRef;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

@Document("gOTerm")
public class GOTermMongo implements Serializable {
    @Id
    private Long id;
    private String name;
    private String namespace;
    private String definition;
    @DBRef
    private Set<GOTermMongo> is_a = new HashSet<>();

    public GOTermMongo() {
    }

    public GOTermMongo(Long id, String name, String namespace, String definition, Set<GOTermMongo> is_a) {
        this.id = id;
        this.name = name;
        this.namespace = namespace;
        this.definition = definition;
        this.is_a = is_a;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getGOid() {
        return id;
    }

    public void setGOid(Long id) {
        this.id = id;
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

    public Set<GOTermMongo> getIs_a() {
        return is_a;
    }

    public void setIs_a(Set<GOTermMongo> is_a) {
        this.is_a = is_a;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        GOTermMongo goTermMongo = (GOTermMongo) o;

        return id.equals(goTermMongo.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    public GOTermMongo(Long id) {
        this.id = id;
    }

}
