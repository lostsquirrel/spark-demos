package demo.spark.sql;

import lombok.Data;

import java.io.Serializable;

@Data
public class Person implements Serializable {
    private String name;
    private int age;
}
