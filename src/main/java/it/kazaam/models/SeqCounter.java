package it.kazaam.models;


import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "SeqCounter")
public class SeqCounter {
    @Id
    private String id;

    private long seq;

    public SeqCounter(String id, long seq) {
        this.id = id;
        this.setSeq(seq);
    }

	public long getSeq() {
		return seq;
	}

	public void setSeq(long seq) {
		this.seq = seq;
	}
}
