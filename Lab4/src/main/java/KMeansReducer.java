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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.filecache.DistributedCache;

public class KMeansReducer extends Reducer<LongWritable, Text, LongWritable, Text> {
    // 存储旧的聚类中心的二维数组
    private double[][] oldCentroids;

    // 设置 Reducer 的初始化方法，从分布式缓存中读取旧聚类中心文件
    @Override
    protected void setup(Context context) throws IOException {
        // 从分布式缓存读取旧聚类中心
        URI[] cacheFiles = DistributedCache.getCacheFiles(context.getConfiguration());
        if (cacheFiles != null && cacheFiles.length > 0) {
            Path centroidFilePath = new Path(cacheFiles[0]);
            FileSystem fs = FileSystem.get(context.getConfiguration());
            oldCentroids = readCentroids(fs, centroidFilePath);
        }
    }

    // reduce 方法：计算每个聚类的新均值，并检查是否有更新
    @Override
    protected void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<double[]> points = new ArrayList<>();

        // 将属于当前聚类的数据点添加到列表
        for (Text value : values) {
            String line = value.toString().trim();
            String[] parts = line.split("[:\t]");
            if (parts.length < 2) return; // 确保数据格式正确
            String vectorPart = parts[1].trim();
            String[] vector = vectorPart.split(",");
            double[] point = new double[vector.length];
            for (int i = 0; i < vector.length; i++) {
                point[i] = Double.parseDouble(vector[i]);
            }
            points.add(point);
        }

        // 计算新的聚类中心
        double[] newCentroid = calculateNewCentroid(points);
        StringBuilder centroidStr = new StringBuilder();
        for (double cord : newCentroid) {
            centroidStr.append(cord).append(",");
        }

        // 移除最后一个多余的逗号
        if (centroidStr.length() > 0) {
            centroidStr.setLength(centroidStr.length() - 1);
        }

        // 输出格式为 "id+\t向量部分"
        String outputValue = centroidStr.toString();


        // 检查新的聚类中心是否与旧的聚类中心相同
        boolean updated = false;
        for (double[] oldCentroid : oldCentroids) {
            if (!equals(newCentroid, oldCentroid)) {
                updated = true;
                break;
            }
        }

        // 如果新的聚类中心有变化，设置更新标志
        if (updated) {
            context.getConfiguration().setBoolean("centroids.updated", true);
        }

        // 输出新的聚类中心
        context.write(key, new Text(outputValue));
    }

    // 计算新的聚类中心
    private double[] calculateNewCentroid(List<double[]> points) {
        int dimensions = points.get(0).length;
        double[] centroid = new double[dimensions];

        // 对每个数据点的每个维度进行求和
        for (double[] point : points) {
            for (int i = 0; i < dimensions; i++) {
                centroid[i] += point[i];
            }
        }

        // 计算每个维度的平均值
        for (int i = 0; i < dimensions; i++) {
            centroid[i] /= points.size();
        }

        return centroid;
    }

    // 比较两个聚类中心是否相同
    private boolean equals(double[] a, double[] b) {
        if (a.length != b.length) return false;
        for (int i = 0; i < a.length; i++) {
            if (Math.abs(a[i] - b[i]) > 1e-6) return false;
        }
        return true;
    }

    // 读取聚类中心数据
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
