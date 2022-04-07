package com.hc.data_to_hudi.until;

public class Dao2 {
    private Long id;
    private String name;

    public Dao2() {
    }

    @Override
    public String toString() {
        return "Dao2{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }

    public Dao2(Long id, String name) {
        this.id = id;
        this.name = name;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }


}
