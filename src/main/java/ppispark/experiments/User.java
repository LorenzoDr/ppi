package experiments;

public class User {

	private String id;
    private String name;
    private int age;
    public User(String id,String name,int age){
        this.id=id;
        this.age=age;
        this.name=name;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public String getName() {
        return name;
    }

    public String getId() {
        return id;
    }
}
