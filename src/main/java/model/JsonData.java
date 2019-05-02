package model;

public class JsonData {
    
    private Long id;
    private String data;
    
    public JsonData(Long id, String data) {
        this.id = id;
        this.data = data;
    }
    
    public Long getId() {
        return id;
    }
    
    public void setId(Long id) {
        this.id = id;
    }
    
    public String getData() {
        return data;
    }
    
    public void setData(String s) {
        this.data = s;
    }
    
    public String toString() {
        return String.format("[JsonData] id = %d\n data = %s\n", id, data.toString());
    }

}
