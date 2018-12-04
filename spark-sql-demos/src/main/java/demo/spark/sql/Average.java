package demo.spark.sql;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Average {
    private long sum;
    private long count;

}
