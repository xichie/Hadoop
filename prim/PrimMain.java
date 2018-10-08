package prim;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PrimMain {

	/*
	 * function:mapReduce主程序
	 * parameter0:args[0]设置最小生成树的起始点,存放在一个名为selectedNode.txt的文件，每次迭代后，会把新选择的点更新到这个文件中
	 * parameter1:args[1] 输入文件中连通图的每一行记录的格式：起点+tab键+终点+tab键+权重
	 * parameter2:args[2]设置输出文件的路径，最后选择的边会在outputPath中保存 
	 * parameter3:args[3] 连通图的点的个数
	 */
	public static void main(String[] args) throws Exception {

		if (args.length < 4) {
			System.err.println("usage: <selectedNodePath, inputPath, outputPath, nodeNumber>");
			return;
		}

		Configuration conf = new Configuration();
		conf.set("selectedNodePath", args[0]);

		String inputPath = args[1];
		String outputPath = args[2];
		int nodeNumber = Integer.parseInt(args[3]);
		FileSystem fs = FileSystem.get(conf);
		while (nodeNumber > 0) {
			conf.setInt("nodeNumber", nodeNumber);
			Job job = Job.getInstance(conf);

			job.setJarByClass(PrimMain.class);

			job.setNumReduceTasks(1); // set number of ReduceTask is 1

			job.setMapperClass(PrimMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Edge.class);

			job.setReducerClass(PrimReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Edge.class);

			FileInputFormat.setInputPaths(job, new Path(inputPath));
			FileOutputFormat.setOutputPath(job, new Path(outputPath + "/" + nodeNumber + "/"));

			job.waitForCompletion(true);
			nodeNumber--;
			updateSelectNode(nodeNumber, outputPath, conf, fs);

		}
		combineAllPartToOne(Integer.parseInt(args[3]), outputPath, conf, fs);

	}

	/*
	 * function:将每次选择的点放到selectedNode.txt中 
	 * parameter0：nodeNumber 连通图的点数
	 * parameter1：每次执行mapreduce输出结果所在的文件夹 
	 * parameter2: conf 配置文件
	 * parameter3: fs 文件系统
	 */
	private static void updateSelectNode(int nodeNumber, String outputPath, Configuration conf, FileSystem fs)
			throws Exception {
		FSDataInputStream in = null;
		FSDataOutputStream out = null;
		try {
			in = fs.open(new Path(outputPath + "/" + (nodeNumber + 1) + "/part-r-00000"));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			out = fs.append(new Path(conf.get("selectedNodePath") + "/selectedNode.txt"));
			String line = null;
			while ((line = br.readLine()) != null) {
				String[] selectedNodes = line.split("\t");
				for (String selectedNode : selectedNodes) {
					out.writeBytes(selectedNode);
					out.writeBytes("\r");
				}
			}
		} finally {
			IOUtils.closeStream(in);
			IOUtils.closeStream(out);
		}
	}

	/*
	 * function:将最后选择的点和对应的权重合并到一个文件里 
	 * parameter0：nodeNumber 连通图的点数
	 * parameter1： 每次执行mapreduce输出结果所在的文件夹
	 * parameter2: conf 配置文件
	 * parameter3: fs 文件系统
	 */
	private static void combineAllPartToOne(int nodeNumber, String outputPath, Configuration conf, FileSystem fs)
			throws Exception {
		while (nodeNumber > 1) {
			FSDataInputStream in = null;
			FSDataOutputStream out = null;
			try {
				in = fs.open(new Path(outputPath + "/" + nodeNumber + "/part-r-00000"));
				BufferedReader br = new BufferedReader(new InputStreamReader(in));
				out = fs.append(new Path(outputPath + "/" + 1 + "/part-r-00000"));
				String line = null;
				while ((line = br.readLine()) != null) {
					String[] Nodes = line.split("\t");
					out.writeBytes(Nodes[1]);
					out.writeBytes("\t");
					out.writeBytes(Nodes[2]);
					out.writeBytes("\t");
					out.writeBytes(Nodes[3]);
					out.writeBytes("\t");
					out.writeBytes("\r");
				}
			} finally {
				IOUtils.closeStream(in);
				IOUtils.closeStream(out);
			}
			nodeNumber--;
		}
	}

}
