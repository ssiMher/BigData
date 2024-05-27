import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TriangleCount {

    public static class TriangleMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text node = new Text();
        private Text connectedNode = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split("\t");
            if (nodes.length == 2) {
                node.set(nodes[0]);
                connectedNode.set(nodes[1]);
                context.write(node, connectedNode);
            }
        }
    }

    public static class TriangleReducer extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();
        private Map<String, Set<String>> edgesMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> uniqueNodes = new HashSet<>();
            for (Text value : values) {
                uniqueNodes.add(value.toString());
            }
            edgesMap.put(key.toString(), uniqueNodes);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int triangleCount = 0;
            for (Map.Entry<String, Set<String>> entry : edgesMap.entrySet()) {
                Set<String> neighborsA = entry.getValue();
                for (String nodeB : neighborsA) {
                    Set<String> neighborsB = edgesMap.get(nodeB);
                    if (neighborsB != null) {
                        for (String nodeC : neighborsB) {
                            if (neighborsA.contains(nodeC)) {
                                triangleCount++;
                            }
                        }
                    }
                }
            }
            result.set(triangleCount);
            context.write(new Text("Triangle Count"), result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Triangle Count");
        job.setJarByClass(TriangleCount.class);
        job.setMapperClass(TriangleMapper.class);
        job.setReducerClass(TriangleReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
