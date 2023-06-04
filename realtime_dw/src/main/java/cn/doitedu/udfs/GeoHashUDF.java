package cn.doitedu.udfs;

import ch.hsr.geohash.GeoHash;
import org.apache.flink.table.functions.ScalarFunction;

public class GeoHashUDF extends ScalarFunction {

    // 接受一个gps座标，返回它的geohash码
    // Double ，就允许接受null
    // 如果是 double，就不允许接受到null
    public String eval(Double lat,Double lng){

        String geohash = null;
        try{
            geohash = GeoHash.geoHashStringWithCharacterPrecision(lat,lng,5);
        }catch (Exception e){
        }
        return geohash;
    }

}
