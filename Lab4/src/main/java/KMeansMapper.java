import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.filecache.DistributedCache;

public class KMeansMapper extends Mapper<LongWritable, Text, LongWritable, Text> {
    // 用于存储聚类中心的二维数组
    private double[][] centroids;

    // 设置 Mapper 的初始化方法，从分布式缓存中读取聚类中心文件
    @Override
    protected void setup(Context context) throws IOException {
        URI[] cacheFiles = DistributedCache.getCacheFiles(context.getConfiguration());

        if (cacheFiles != null && cacheFiles.length > 0) {
            Path centroidFilePath = new Path(cacheFiles[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            centroids = readCentroids(fs, centroidFilePath);
        }
    }

    // map 方法：对每个输入数据点计算其到各个聚类中心的距离，并将其分配到最近的聚类中心
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 拆分ID和向量
        String[] parts = value.toString().split("[:\t]");
        if (parts.length < 2) return; // 确保数据格式正确
        String vectorPart = parts[1].trim();

        // 将向量部分转换为数值
        String[] vector = vectorPart.split(",");
        double[] point = new double[vector.length];
        for (int i = 0; i < vector.length; i++) {
            point[i] = Double.parseDouble(vector[i].trim());
        }
        // 找到最近的聚类中心
        int closestCentroid = getClosestCentroid(point);

        // 将数据点输出到最近的聚类中心
        context.write(new LongWritable(closestCentroid), value);
    }

    // 获取数据点到最近的聚类中心的索引
    private int getClosestCentroid(double[] point) {
        double minDistance = Double.MAX_VALUE;
        int closestCentroid = -1;
        // 遍历所有聚类中心，计算每个中心到数据点的距离
        for (int i = 0; i < centroids.length; i++) {
            double distance = 0;
            for (int j = 0; j < point.length; j++) {
                distance += Math.pow(point[j] - centroids[i][j], 2);
            }
            // 更新到目前为止最近的聚类中心
            if (distance < minDistance) {
                minDistance = distance;
                closestCentroid = i;
            }
        }
        return closestCentroid;
    }

    // 从文件中读取聚类中心数据
    private double[][] readCentroids(FileSystem fs,Path path) throws IOException {
        List<double[]> centroidList = new ArrayList<>();
        // 使用 BufferedReader 读取文件内容
        BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
        String line;
        // 逐行读取聚类中心数据
        while ((line = br.readLine()) != null) {
            String[] parts = line.split("[:\t]");
            if (parts.length < 2) continue; // 确保数据格式正确
            String vectorPart = parts[1].trim();
            // 将向量部分按逗号分隔，转换为 double 数组
            String[] tokens = vectorPart.split(",");
            double[] centroid = new double[tokens.length];
            for (int i = 0; i < tokens.length; i++) {
                centroid[i] = Double.parseDouble(tokens[i]);
            }
            centroidList.add(centroid);
        }
        br.close();
        // 将列表转换为二维 double 数组并返回
        return centroidList.toArray(new double[0][0]);
    }
}
