import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

public class Invert01 {
    public static class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text>
    {
        Text res = new Text();
        @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException
// default RecordReader: LineRecordReader; key: line offset; value: line string
    {
        FileSplit fileSplit = (FileSplit)context.getInputSplit();
        String fileName = fileSplit.getPath().getName();
        Text word = new Text();
        //Text fileName_lineOffset = new Text(fileName+"#"+key.toString());
        //StringTokenizer itr = new StringTokenizer(value.toString());
        StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().replaceAll("[^0-9a-zA-Z ]"," "));
        Hashtable<Text, IntWritable> map = new Hashtable<Text, IntWritable>();
        //StringTokenizer itr = new StringTokenizer(value.toString());
        for(; itr.hasMoreTokens(); )
        {
            word.set(itr.nextToken());
            if(map.containsKey(word)){
                map.put(word, new IntWritable(map.get(word).get() + 1));
            }
            else{
                map.put(word, new IntWritable(1));
            }
        }

        for (Text map_2 : map.keySet()){
            res.set(fileName + "," + map.get(map_2).toString());
            context.write(map_2, res);
        }
    }
    }
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
    {
        //Text res = new Text();
        @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
    {
        //Iterator<Text> it = values.iterator();

        Hashtable<String, Integer> map = new Hashtable<String, Integer>();

        //Text word = new Text();
        int fileCnt = 0, sumCnt = 0;
        for(Text t: values )
        {
            //Text tmp = it.next();
            String file = t.toString().split(",")[0];
            int result = Integer.parseInt(t.toString().split(",")[1]);
            sumCnt+=result;
            //word.set(t.toString().split(",")[0]);
            if(map.containsKey(file)){
                map.put(file,map.get(file)+ result);
            }
            else{
                fileCnt++;
                map.put(file, result);
            }
        }
        //Iterator<Text> iterator = map.keySet().iterator();
        StringBuilder all = new StringBuilder();
        double avg = (double)sumCnt/fileCnt;
        String str_average = String.format("%.2f",avg);
        all.append("\t" + str_average + ",");
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            all.append(entry.getKey());
            all.append(":");
            all.append(entry.getValue().toString());
            all.append(";");
            //System.out.println("key = " + entry.getKey() + ", value = " + entry.getValue());
        }

        context.write(key, new Text(all.toString()));
    } //最终输出键值对示例：("fish", “doc1#0; doc1#8;doc2#0;doc2#8 ")
    }
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job = new Job(conf, "invert index");
            job.setJarByClass(Invert01.class);
            job.setInputFormatClass(TextInputFormat.class);
            job.setMapperClass(Invert01.InvertedIndexMapper.class);
            job.setReducerClass(Invert01.InvertedIndexReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (Exception e) { e.printStackTrace(); }
    }
}
