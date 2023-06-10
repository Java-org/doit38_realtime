package cn.doitedu.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.Collector;

public class CogroupTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointStorage("file:/d:/ckpt/");

        DataStreamSource<A> s1 = env.addSource(new MySource1()).setParallelism(2);
        DataStreamSource<B> s2 = env.addSource(new MySource2()).setParallelism(2);


        s1.connect(s2)
                .process(new CoProcessFunction<A, B, String>() {
                    @Override
                    public void processElement1(A a, CoProcessFunction<A, B, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(a.toString());
                    }

                    @Override
                    public void processElement2(B b, CoProcessFunction<A, B, String>.Context context, Collector<String> collector) throws Exception {
                        collector.collect(b.toString());
                    }
                }).print();


        env.execute();


    }

    public static class MySource1 extends RichParallelSourceFunction<A> {

        @Override
        public void run(SourceContext<A> sourceContext) throws Exception {
            while (true) {
                sourceContext.collect(new A(RandomStringUtils.randomAlphabetic(1).toLowerCase(), RandomUtils.nextInt(1, 10), System.currentTimeMillis()));
                Thread.sleep(RandomUtils.nextInt(100, 1000));
            }
        }

        @Override
        public void cancel() {

        }
    }

    public static class MySource2 extends RichParallelSourceFunction<B> {

        @Override
        public void run(SourceContext<B> sourceContext) throws Exception {
            while (true) {
                sourceContext.collect(new B(RandomStringUtils.randomAlphabetic(1).toLowerCase(), RandomUtils.nextInt(100, 200), System.currentTimeMillis()));
                Thread.sleep(RandomUtils.nextInt(100, 1000));
            }
        }

        @Override
        public void cancel() {

        }
    }


    @Data
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class A {
        private String id;
        private int fa;
        private long eventTime;
    }


    @Data
    @Setter
    @NoArgsConstructor
    @AllArgsConstructor
    public static class B {
        private String id;
        private int fb;
        private long eventTime;
    }

}
