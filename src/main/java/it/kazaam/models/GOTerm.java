package it.kazaam.models;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class GOTerm implements Serializable {
    private Long GOid;
    private String name;
    private String namespace;
    private String definition;
    private Set<GOTerm> is_a = new HashSet<>();

    public GOTerm() {
    }

    public GOTerm(Long GOid) {
        this.GOid = GOid;
    }

    public GOTerm(Long GOid, String name, String namespace, String definition, Set<GOTerm> is_a) {
        this.GOid = GOid;
        this.name = name;
        this.namespace = namespace;
        this.definition = definition;
        this.is_a = is_a;
    }

    /**
     * Converti i blocchi dei termini in formato obo in oggetti Term
     *
     * @param block Blocck nel formato obo [Term].
     * @return Un oggetto Term
     */
    public static GOTerm convertOBOBlock(String block) {
        GOTerm result = new GOTerm();
        String[] lines = block.split("\\n");
        if ("[Typedef]".equals(lines[0])) return null;
        for (String line : lines) {
            String[] values = line.split(":");
            switch (values[0]) {
                case "id":
                    result.setGOid(Long.parseLong(values[2].trim()));
                    break;
                case "name":
                    result.setName(values[1].trim());
                    break;
                case "namespace":
                    result.setNamespace(values[1].trim());
                    break;
                case "def":
                    values[0] = "";
                    result.setDefinition(String.join(":", values).trim());
                    break;
                case "is_a":
                    values[0] = "";
                    String value = String.join(":", values).substring(2).trim();
                    result.is_a.add(new GOTerm(Long.parseLong(value.split("!")[0].split(":")[1].trim())));
                    break;
            }
        }
        return result;
    }

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

    public Set<GOTerm> getIs_a() {
        return is_a;
    }

    public void setIs_a(Set<GOTerm> is_a) {
        this.is_a = is_a;
    }

    @Override
    public String toString() {
        return "Term{" +
                "id='" + GOid + '\'' +
                ", name='" + name + '\'' +
                ", namespace='" + namespace + '\'' +
                ", description='" + definition + '\'' +
                //", is_a=" + is_a +
                '}';
    }
}
