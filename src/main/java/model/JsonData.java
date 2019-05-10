package model;

import org.h2.util.json.JSONValue;

public class JsonData {

    private Long id;
    private JSONValue data;

    public JsonData(Long id, JSONValue data) {
        this.id = id;
        this.data = data;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public JSONValue getData() {
        return data;
    }

    public void setData(JSONValue s) {
        this.data = s;
    }

    public String toString() {
        return String.format("[JsonData] id = %d\n data = %s\n", id, data.toString());
    }

}
