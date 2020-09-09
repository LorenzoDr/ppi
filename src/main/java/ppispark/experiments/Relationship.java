package experiments;

public class Relationship {
	
	 private String type;
	 private String src;
	 private String dst;
	
    public Relationship(String type,String src,String dst){
	    this.type=type;
	    this.dst=dst;
	    this.src=src;
	}

    
	public void setDst(String dst) {
	    this.dst = dst;
	}

	public void setSrc(String src) {
	    this.src = src;
	}

	public void setType(String type) {
	    this.type = type;
	}

	public String getDst() {
	    return dst;
	}
	
	public String getType() {
	    return type;
	}
	
	public String  getSrc() {
	    return src;
	}

}
