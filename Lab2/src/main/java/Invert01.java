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
        {
            FileSplit fileSplit = (FileSplit)context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            Text word = new Text();
            //获取当前正在处理的文件名，并为存储 Mapper 输出的单词或键值对的键部分创建一个 Text 对象
            StringTokenizer itr = new StringTokenizer(value.toString().toLowerCase().replaceAll("[^0-9a-zA-Z ]"," "));
            Hashtable<Text, IntWritable> map = new Hashtable<Text, IntWritable>();
            /*将输入的文本数据转换为小写形式，并将非字母数字字符替换为空格，然后逐个处理文本中的单词，将每个单词作为键，
            其出现次数作为值，存储在一个 Hashtable 对象中。*/
            for(; itr.hasMoreTokens(); )
            {
                word.set(itr.nextToken());
                if(map.containsKey(word)){
                    map.put(word, new IntWritable(map.get(word).get() + 1));
                    //用于检查 Hashtable 对象 map 中是否已经包含了当前单词。如果 map 中已经包含了当前单词，则将该单词的出现次数加一
                }
                else{
                    map.put(word, new IntWritable(1));
                    //如果 map 中没有包含当前单词，则将当前单词添加到 map 中，并将其出现次数设置为1
                }
            }
            for (Text map_2 : map.keySet()){
                res.set(fileName + "," + map.get(map_2).toString());
                context.write(map_2, res);
            }
            //将每个单词及其在文件中出现的次数写入到 Mapper 的输出中
        }
    }
    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
    {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException
        {
            Hashtable<String, Integer> map = new Hashtable<String, Integer>();
            //用于存储单词及其出现次数
            int fileCnt = 0, sumCnt = 0;//分别用于统计文件数量和单词总数。
            for(Text t: values )
            {
                String file = t.toString().split(",")[0];
                //当前值Text t转换为字符串，并通过逗号分割，提取出文件名
                int result = Integer.parseInt(t.toString().split(",")[1]);
                //从当前值Text t中提取出出现次数，并将其转换为整数类型
                sumCnt+=result;//将当前单词在所有文件中的出现次数累加到sumCnt中
                if(map.containsKey(file))//用于检查map中是否已经包含了当前文件名
                {
                    map.put(file,map.get(file)+ result);
                    //第一次遇到该文件名，将其添加到map中，并将对应的值设置为当前值
                }
                else{
                    fileCnt++;//统计文件的数量
                    map.put(file, result);//更新
                }
            }
            StringBuilder all = new StringBuilder();
            //构建一个字符串，以便后续将文件名、单词出现次数等信息添加到其中。
            double avg = (double)sumCnt/fileCnt;//单词的平均出现次数
            String str_average = String.format("%.2f",avg);
            all.append("\t" + str_average + ",");
            //all.append(str_average + ",");
            for (Map.Entry<String, Integer> entry : map.entrySet()) {
                all.append(entry.getKey());//将文件名加入
                all.append(":");
                all.append(entry.getValue().toString());//加入出现次数
                all.append(";");
            }

            context.write(key, new Text(all.toString()));
            //使用Reducer的上下文对象context将结果写入到输出中。
        }
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
