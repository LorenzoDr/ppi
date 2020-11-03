package it.kazaam.models;

import org.bson.types.ObjectId;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.io.Serializable;
import java.util.Objects;

@Document
public class Annotation implements Serializable {
    @Id
    private ObjectId _id;
    @Indexed
    private String annotationID;
    private String db;
    @Indexed
    private Long goID;
    private String aspect;
    private String symbol;
    private String name;
    private String type;

    public Annotation() {
    }

    public Annotation(String[] values) {
        annotationID = values[1];
        db = values[0];
        goID = Long.parseLong(values[4].trim().split(":")[1]);
        aspect = AnnotationType.valueOf(values[8]).type;
        symbol = values[2];
        name = values[9];
        type = values[11];
    }

    public String getAnnotationID() {
        return annotationID;
    }

    public ObjectId get_id() {
        return _id;
    }

    public void set_id(ObjectId _id) {
        this._id = _id;
    }

    public void setAnnotationID(String annotationID) {
        this.annotationID = annotationID;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    public Long getGoID() {
        return goID;
    }

    public void setGoID(Long goID) {
        this.goID = goID;
    }

    public String getAspect() {
        return aspect;
    }

    public void setAspect(String aspect) {
        this.aspect = aspect;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Annotation that = (Annotation) o;
        return Objects.equals(annotationID, that.annotationID) &&
                Objects.equals(db, that.db) &&
                Objects.equals(goID, that.goID) &&
                Objects.equals(aspect, that.aspect);
    }

    @Override
    public int hashCode() {
        return Objects.hash(annotationID, db, goID, aspect);
    }

    @Override
    public String toString() {
        return "Annotation{" +
                "_id=" + _id +
                ", annotationID='" + annotationID + '\'' +
                ", db='" + db + '\'' +
                ", goID='" + goID + '\'' +
                ", aspect='" + aspect + '\'' +
                ", symbol='" + symbol + '\'' +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                '}';
    }

    enum AnnotationType {
        F("molecular function"),
        P("biological process"),
        C("cellular component");

        public final String type;

        private AnnotationType(String type) {
            this.type = type;
        }
    }
}
