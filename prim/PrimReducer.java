package prim;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PrimReducer extends Reducer<Text, Edge, Text, Edge> {

	/*
	 * function：将map输出的node对象选择权重最小node对象输出 
	 * parameter0: k2 map的输出的键（即1） 
	 * parameter1:v2 map筛选后的所有node对象 
	 * parameter2: context 上下文 
	 * output:key 最小权重的node对象的起点 value 最小权重的node对象
	 */
	protected void reduce(Text k2, Iterable<Edge> v2, Context context) throws IOException, InterruptedException {
		if (v2 != null) {
			Edge minNode = getMinNode(v2);
			if (minNode != null) {
				context.write(new Text(minNode.getNode1()), minNode);
			}
		}

	}

	/*
	 * function:在包含所有node对象中选择权重最小的node对象 
	 * parameter：iter 包含所有node对象的容器
	 * return：容器中权重最小的node对象
	 */
	public Edge getMinNode(Iterable<Edge> iter) {
		Iterator<Edge> iterator = iter.iterator();
		if (!iterator.hasNext()) {
			return null;
		}
		Edge minNode = new Edge(iterator.next());
		while (iterator.hasNext()) {
			Edge nextNode = iterator.next();
			if ((nextNode.getEdge() < minNode.getEdge())) {
				minNode = new Edge(nextNode);
			}
		}
		return minNode;

	}
}
