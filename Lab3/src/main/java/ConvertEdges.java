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
    //用于处理输入数据中的边（Edge）信息
    public static class EdgeMapper extends Mapper<LongWritable, Text, Text, Text> {
        private Text nodeA = new Text();//边的起始节点
        private Text nodeB = new Text();//边的终止节点

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //将输入的Text类型的value转换为字符串，并按照", "进行分割，得到两个节点
            String[] nodes = value.toString().split(", ");

            if (nodes.length == 2) {//如果成功分割出两个节点,将第一个节点设置为nodeA,第二个节点设置为nodeB
                nodeA.set(nodes[0]);
                nodeB.set(nodes[1]);
                context.write(nodeA, nodeB);
            }
        }
    }

    public static class EdgeReducer extends Reducer<Text, Text, Text, Text> {
        private Map<String, Set<String>> edgesMap = new HashMap<>();//存储每个节点（key）与其相连节点（value）的映射关系

        //处理具有相同key的values集合
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String nodeA = key.toString();//获取当前key的值，即节点A
            Set<String> connectedNodes = new HashSet<>();//创建一个HashSet来存储与节点A相连的所有节点
            for (Text value : values) {//遍历与节点A相连的所有节点,将这些节点添加到connectedNodes集合中
                connectedNodes.add(value.toString());
            }
            edgesMap.put(nodeA, connectedNodes);//将节点A及其相连节点集合存入edgesMap中
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            for (Map.Entry<String, Set<String>> entry : edgesMap.entrySet()) {//遍历edgesMap中的每一个条目
                String nodeA = entry.getKey();//获取当前条目的key，即节点A
                Set<String> connectedNodesA = entry.getValue();
                for (String nodeB : connectedNodesA) {//遍历与节点A相连的每个节点
                    //如果节点A小于节点B，则输出(A, B)，否则输出(B, A)，以确保边的顺序统一
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
