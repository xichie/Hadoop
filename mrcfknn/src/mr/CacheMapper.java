package mr;

import main.FuzzyKnn;
import main.Instance;
import main.Knn;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

public class CacheMapper extends Mapper<LongWritable, Text, NullWritable, Text> {
    ArrayList<Instance> subSet = new ArrayList<>();         //选择子集
    FuzzyKnn fknn;
    int k = 5;          // 近邻
    int m = 2;          // 范数
    double e;           // 熵的阈值

    /*
        初始化k、m
        加载已选择的数据子集subSet
     */
    @Override
    protected void setup(Context context)
            throws IOException {
        String label = context.getConfiguration().get("label", "");     // 获得类别信息
        k = context.getConfiguration().getInt("k", 5);                  // 获得k
        m = context.getConfiguration().getInt("m", 2);                  // 获得m
        String[] labels = label.trim().split(":");                                  // 按-把类别信息分开
        for(int i = 0; i < labels.length; i++){
            labels[i] = (int)Double.parseDouble(labels[i]) + "";
        }
        fknn = new FuzzyKnn(k, m);
        fknn.labels = labels;

        e = context.getConfiguration().getDouble("entropy", 0.5);       // 加载熵的阈值

        //加载数据子集


    }

    @Override
    protected void map(LongWritable key, Text value, Context context){
        Instance newInstance = new Instance(value.toString());      // 从待选数据中读取一个新的样例
        try {
            Instance[] knn = Knn.findKNN(newInstance, subSet, fknn.k);      // 在已选数据中找新样例的k个近邻
//            System.out.println(knn.length);
//            System.out.println(knn[0].toString());
            if(knn != null) {                                       // 如果子集中不包含待加入的样例
                ArrayList<Instance> kNearestNeighbor = new ArrayList<>();
                for (int j = 0; j < knn.length; j++) {
                    kNearestNeighbor.add(knn[j]);
                }
                double entropy = fknn.run(kNearestNeighbor, newInstance);       // 计算新样例的熵值
//                System.out.println(entropy);
//                System.out.println("---------------------------------");
                if (entropy > e) {
                    // 如果大于熵的阈值则输出该样例
                    System.out.println("熵：" + entropy);
                    context.write(NullWritable.get(), new Text(newInstance.toString()));
                }
            }
        } catch (Exception e) {
            System.out.println("没有找到K个近邻");
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for(Instance ins : subSet){
            context.write(NullWritable.get(), new Text(ins.toString()));
        }
    }
}