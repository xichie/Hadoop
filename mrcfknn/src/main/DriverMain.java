package main;

import mr.CfknnCombiner;
import mr.CfknnMapper;
import mr.CfknnReducer;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.net.URI;


public class DriverMain {

    /*
        输入文件分为一个初始化的子集和一个大数据集

        两个文件放到basePath目录下的outputs/0, 和input目录下
		
		参数：
			迭代次数
			类别信息(-号)  例如： 1.00000000-2.00000000   一定要完全和数据中类别一样。
			k
			文件路径
			熵初始阈值
			衰减步长
     */
    public static void main(String[] args) {

//       System.setProperty("hadoop.home.dir", "E:\\Hadoop\\temp\\winutils-master\\hadoop-2.7.1\\");
        long startTime = System.currentTimeMillis();
        if (args.length < 5) {
            System.out.println("迭代次数\r\n类别信息(:号)\r\nk\r\n文件路径\r\n熵初始阈值\r\ndecay");
            System.exit(-1);
        }
        int iterNum = Integer.parseInt(args[0]);
        String label = args[1];
        System.out.println(label);
        int k = Integer.parseInt(args[2]);
        String basePath = args[3];
        double entropy = Double.parseDouble(args[4]);
        double decay = Double.parseDouble(args[5]);

        Configuration conf = new Configuration();
        conf.set("mapreduce.map.cpu.vcores", "4");      // 设置map最大cpu占用个数。默认为1
        conf.set("mapreduce.map.memory.mb", "30000");    // 设置map最大内存占用单位MB
        conf.set("mapreduce.reduce.memory.mb", "10240");
//        conf.set("yarn.nodemanager.resource.memory-mb", "10240");
//        conf.set("yarn.scheduler.minimum-allocation-mb", "10240");
//        conf.set("yarn.scheduler.maximum-allocation-mb", "20480");
//        conf.set("yarn.app.mapreduce.am.resource.mb", "10240");
//        conf.set("yarn.app.mapreduce.am.command-opts", "-Xmx25600m");

        //        conf.setInt("mapred.max.split.size", 10485760);  //单位是字节，物理块是10M
        System.out.println("————————程序开始执行————————22");
        for (int i = 0; i < iterNum; i++)
            if (entropy > 0) {
                System.out.printf("开始第%d次迭代......", i + 1);
                try {
                    Job job = Job.getInstance(conf);
                    job.setJobName("CFKNN_" + i + "entropy: " + entropy);
                    job.getConfiguration().set("label", label);
                    job.getConfiguration().setInt("k", k);
                    job.getConfiguration().set("subSetPath", basePath + "/outputs/" + i + "/");
                    job.getConfiguration().setDouble("entropy", entropy);
                    job.addCacheFile(new URI(basePath + "/outputs/" + i + "/"));


                    job.setJarByClass(DriverMain.class);

                    job.setMapperClass(CfknnMapper.class);
                    job.setMapOutputKeyClass(NullWritable.class);
                    job.setMapOutputValueClass(Text.class);

                    job.setCombinerClass(CfknnCombiner.class);

                    job.setReducerClass(CfknnReducer.class);
                    job.setOutputKeyClass(NullWritable.class);
                    job.setOutputValueClass(Text.class);

                    Path outPath = new Path(basePath + "/outputs/" + (i + 1));
                    FileSystem fs = FileSystem.get(conf);
                    if (fs.exists(outPath)) {
                        fs.delete(outPath, true);
                    }
                    FileInputFormat.setInputPaths(job, new Path(basePath + "/input"));
                    FileOutputFormat.setOutputPath(job, outPath);

                    job.waitForCompletion(true);
                    entropy = entropy - decay;       // 权重衰减

                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        long stopTime = System.currentTimeMillis();
        System.out.println(stopTime - startTime);
        System.out.println("————————程序执行结束————————");
    }
}
