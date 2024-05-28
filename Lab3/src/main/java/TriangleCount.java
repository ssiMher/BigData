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
        private Text node = new Text();//用于存储当前节点
        private Text connectedNode = new Text();//用于存储与当前节点相连的节点

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] nodes = value.toString().split("\t");//将输入的Text类型的value转换为字符串，并按制表符"\t"进行分割
            if (nodes.length == 2) {//如果分割后的数组长度为2（即格式正确，有一个节点和一个连接的节点）
                //将第一个节点设置为node，将第二个节点（即与第一个节点相连的节点）设置为connectedNode
                node.set(nodes[0]);
                connectedNode.set(nodes[1]);
                context.write(node, connectedNode);
                //输出键值对到Mapper的上下文中，这样它们就可以被Reducer接收和处理
            }
        }
    }

    public static class TriangleReducer extends Reducer<Text, Text, Text, IntWritable> {
        private IntWritable result = new IntWritable();//用于存储每个节点参与形成的三角形的数量
        private Map<String, Set<String>> edgesMap = new HashMap<>();//用于存储每个节点及其与之相连的唯一节点的集合

        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Set<String> uniqueNodes = new HashSet<>();//用于存储与当前节点相连的唯一节点
            for (Text value : values) {//遍历与当前key相连的所有节点，将每个节点添加到uniqueNodes集合中
                uniqueNodes.add(value.toString());
            }
            edgesMap.put(key.toString(), uniqueNodes);//将当前节点和与之相连的唯一节点集合添加到edgesMap中
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            int triangleCount = 0;//初始化三角形计数器
            //遍历edgesMap中的每个节点（key）和它的邻居节点（value）
            for (Map.Entry<String, Set<String>> entry : edgesMap.entrySet()) {
                Set<String> neighborsA = entry.getValue();//获取当前节点的邻居节点集
                for (String nodeB : neighborsA) {//遍历当前节点的每个邻居节点
                    Set<String> neighborsB = edgesMap.get(nodeB);//获取节点B的邻居节点集合
                    if (neighborsB != null) {
                        for (String nodeC : neighborsB) {//如果节点B的邻居节点集合不为空，遍历节点B的邻居节点
                            //如果节点C也是节点A的邻居节点（即构成了一个三角形a-b-c），计数器加1
                            if (neighborsA.contains(nodeC)) {
                                triangleCount++;
                            }
                        }
                    }
                }
            }
            result.set(triangleCount);//将三角形计数器的值设置到result对象中
            context.write(new Text("a->b and b->a then a-b: TriangleSum = "), result);//输出三角形计数器的值
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
