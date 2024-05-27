import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TriangleDriver {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: TriangleDriver <input path> <intermediate path> <output path>");
            System.exit(-1);
        }

        // Job 1: ConvertEdges
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Convert Edges");
        job1.setJarByClass(TriangleDriver.class);
        job1.setMapperClass(ConvertEdges.EdgeMapper.class);
        job1.setReducerClass(ConvertEdges.EdgeReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        boolean job1Success = job1.waitForCompletion(true);

        if (!job1Success) {
            System.err.println("ConvertEdges job failed");
            System.exit(1);
        }

        // Job 2: TriangleCount
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Triangle Count");
        job2.setJarByClass(TriangleDriver.class);
        job2.setMapperClass(TriangleCount.TriangleMapper.class);
        job2.setReducerClass(TriangleCount.TriangleReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        boolean job2Success = job2.waitForCompletion(true);

        if (!job2Success) {
            System.err.println("TriangleCount job failed");
            System.exit(1);
        }

        System.exit(0);
    }
}
