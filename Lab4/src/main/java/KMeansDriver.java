import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;

public class KMeansDriver {
    public static void main(String[] args) throws Exception {
        // 创建Hadoop配置对象
        Configuration conf = new Configuration();
        // 获取文件系统对象
        FileSystem fs = FileSystem.get(conf);

        // 设置输入路径、输出路径、聚类中心路径和新聚类中心路径
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path centroidPath = new Path(args[2]);
        //Path newCentroidPath = new Path(args[3]);
        Path backupCentroidPath = new Path(centroidPath.toString() + "_backup");

        // 复制初始聚类中心文件以防止原始文件被删除
        copyFile(fs, centroidPath, backupCentroidPath);

        // 初始化收敛标志和迭代次数
        boolean converged = false;
        int iteration = 0;

        // 迭代执行KMeans算法直到聚类中心收敛
        while (!converged) {
            // 每次迭代时创建新的输出目录
            Path iterationOutputPath = new Path(outputPath, "iteration-" + iteration);
            // 如果输出目录已存在，则删除
            if (fs.exists(iterationOutputPath)) {
                fs.delete(iterationOutputPath, true);
            }

            // 创建一个新的MapReduce Job
            Job job = Job.getInstance(conf, "KMeans Clustering");
            // 设置主类
            job.setJarByClass(KMeansDriver.class);
            // 设置Mapper类
            job.setMapperClass(KMeansMapper.class);
            // 设置Reducer类
            job.setReducerClass(KMeansReducer.class);
            // 设置输出键的类型
            job.setOutputKeyClass(LongWritable.class);
            // 设置输出值的类型
            job.setOutputValueClass(Text.class);

            // 设置输入路径
            FileInputFormat.addInputPath(job, inputPath);
            // 设置输出路径，按照迭代次数创建子目录
            FileOutputFormat.setOutputPath(job, iterationOutputPath);

            // 将聚类中心文件添加到分布式缓存中
            DistributedCache.addCacheFile(backupCentroidPath.toUri(), job.getConfiguration());

            // 设置标志，用于判断均值向量是否更新
            job.getConfiguration().setBoolean("centroids.updated", false);

            // 等待作业完成
            job.waitForCompletion(true);

            // 检查均值向量是否更新
            converged = !job.getConfiguration().getBoolean("centroids.updated", true);

            // 如果未收敛，准备下一次迭代
            if (!converged) {
                // 删除旧聚类中心文件
                fs.delete(backupCentroidPath, true);
                // 重命名新聚类中心文件为旧聚类中心文件
                //fs.rename(newCentroidPath, centroidPath);
                // 增加迭代次数
                iteration++;
            }
        }

        // 恢复原始聚类中心文件
        //fs.rename(backupCentroidPath, centroidPath);
    }

    // 将一个文件的内容拷贝到另一个文件中
    public static void copyFile(FileSystem fileSystem, Path path_from, Path path_to) throws IOException {
        //Path path_from = new Path(from_path);
        //Path path_to = new Path(to_path);
        Configuration configuration = new Configuration();
        // 获取HDFS 文件系统接口
        //FileSystem fileSystem = path.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path_from);
        LineReader lineReader = new LineReader(inputStream, configuration);
        FSDataOutputStream outputStream = fileSystem.create(path_to);
        Text line = new Text();
        while(lineReader.readLine(line) > 0) {
            String str = line.toString() + "\n";
            outputStream.write(str.getBytes());
        }
        lineReader.close();
        outputStream.close();
    }
}
