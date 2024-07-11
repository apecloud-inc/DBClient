import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class FlinkExample {

    public static void doTest() throws Exception {
        String username = System.getenv("username");
        String password = System.getenv("password");
        String hostname = System.getenv("hostname");
        String port = System.getenv("port");

        // ip随便命名
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // get input data by connecting to the socket
        DataStream<String> text = env.socketTextStream(hostname, Integer.parseInt(port), "\n");

        // parse the data, group it, window it, and aggregate the counts
        DataStream<WordWithCount> windowCounts =
                text.flatMap(
                                new FlatMapFunction<String, WordWithCount>() {
                                    @Override
                                    public void flatMap(
                                            String value, Collector<WordWithCount> out) {
                                        for (String word : value.split("\\s")) {
                                            out.collect(new WordWithCount(word, 1L));
                                        }
                                    }
                                })
                        .keyBy(value -> value.word)
                        .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                        .reduce(
                                new ReduceFunction<WordWithCount>() {
                                    @Override
                                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                                        return new WordWithCount(a.word, a.count + b.count);
                                    }
                                });

        // print the results with a single thread, rather than in parallel
        windowCounts.print().setParallelism(1);

        env.execute("Socket Window WordCount");
    }

    // ------------------------------------------------------------------------

    /** Data type for words with count. */
    public static class WordWithCount {

        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(String word, long count) {
            this.word = word;
            this.count = count;
        }

        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
