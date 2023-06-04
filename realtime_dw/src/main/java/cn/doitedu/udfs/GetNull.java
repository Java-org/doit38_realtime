package cn.doitedu.udfs;

import org.apache.flink.table.functions.ScalarFunction;

public class GetNull extends ScalarFunction {

    public Long eval(){
        return null;
    }


}
