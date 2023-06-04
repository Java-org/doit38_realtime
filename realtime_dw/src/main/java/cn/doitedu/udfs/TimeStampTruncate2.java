package cn.doitedu.udfs;

import org.apache.flink.table.functions.ScalarFunction;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

public class TimeStampTruncate2 extends ScalarFunction {

    public String eval(Long time, Integer intervalMinutes) {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

        try {
            // 将传入的取整间隔（分钟） ,转成毫秒
            long itv = intervalMinutes * 60 * 1000;

            // (时间戳/取整间隔)*取整间隔
            long truncatedTime = (time / itv) * itv;

            // 把取整后的长整数时间，解析成 格式化的字符串
            String timeStr = sdf.format(new Date(truncatedTime));

            // 返回结果
            return timeStr;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }


}
