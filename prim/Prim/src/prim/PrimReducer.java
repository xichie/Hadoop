package prim;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PrimReducer extends Reducer<Text, Edge, Text, Edge> {
	
	protected void reduce(Text k2, Iterable<Edge> v2, Context context) 
			throws IOException ,InterruptedException {
		if(v2 != null) {
			Edge minNode = getMinNode(v2);
			if (minNode != null) {
				context.write(new Text(minNode.getNode1()), minNode);
			}
		}
		
	}
	public Edge getMinNode(Iterable<Edge> iter) {
		Iterator<Edge> iterator = iter.iterator(); 
		if(!iterator.hasNext()) {
			return null;
		}
		Edge minNode = new Edge(iterator.next());
		while(iterator.hasNext()) {
			Edge nextNode = iterator.next();
			if((nextNode.getEdge() < minNode.getEdge())) { 
				minNode = new Edge(nextNode);
			}
		}
		return minNode;
		
	} 
}
