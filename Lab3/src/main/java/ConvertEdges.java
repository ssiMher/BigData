import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

public class ConvertEdges {

    public static class EdgeMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text nodeA = new Text();
        private Text nodeB = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split(", ");
            if (nodes.length == 2) {
                nodeA.set(nodes[0]);
                nodeB.set(nodes[1]);
                context.write(nodeA, nodeB);
            }
        }
    }

    public static class EdgeReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Set<String>> edgesMap = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String nodeA = key.toString();
            Set<String> connectedNodes = new HashSet<>();
            for (Text value : values) {
                connectedNodes.add(value.toString());
            }
            edgesMap.put(nodeA, connectedNodes);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Set<String>> entry : edgesMap.entrySet()) {
                String nodeA = entry.getKey();
                Set<String> connectedNodesA = entry.getValue();
                for (String nodeB : connectedNodesA) {
                    if (edgesMap.containsKey(nodeB) && edgesMap.get(nodeB).contains(nodeA)) {
                        if (nodeA.compareTo(nodeB) < 0) {
                            context.write(new Text(nodeA), new Text(nodeB));
                        } else {
                            context.write(new Text(nodeB), new Text(nodeA));
                        }
                    }
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Convert Edges");
        job.setJarByClass(ConvertEdges.class);
        job.setMapperClass(EdgeMapper.class);
        job.setReducerClass(EdgeReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
