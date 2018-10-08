package prim;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/*
 * function：将一条边封装成一个可序列化的对象
 * Member variables0: node1 起点
 * Member variables1: node2 终点
 * Member variables2: edge 权重
 * */
public class Edge implements Writable, Comparable<Edge> {
	private String node1;
	private String node2;
	private double edge;

	public Edge(String node1, String node2, double edge) {
		super();
		this.node1 = node1;
		this.node2 = node2;
		this.edge = edge;
	}

	public Edge(Edge node) {
		this.node1 = node.getNode1();
		this.node2 = node.getNode2();
		this.edge = node.getEdge();
	}

	public Edge() {
		super();
	}

	public String getNode1() {
		return node1;
	}

	public void setNode1(String node1) {
		this.node1 = node1;
	}

	public String getNode2() {
		return node2;
	}

	public void setNode2(String node2) {
		this.node2 = node2;
	}

	public double getEdge() {
		return edge;
	}

	public void setEdge(double edge) {
		this.edge = edge;
	}

	@Override
	public String toString() {
		return this.getNode1() + "\t" + this.getNode2() + "\t" + this.getEdge();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(edge);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((node1 == null) ? 0 : node1.hashCode());
		result = prime * result + ((node2 == null) ? 0 : node2.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Edge other = (Edge) obj;
		if (Double.doubleToLongBits(edge) != Double.doubleToLongBits(other.edge))
			return false;
		if (node1 == null) {
			if (other.node1 != null)
				return false;
		} else if (!node1.equals(other.node1))
			return false;
		if (node2 == null) {
			if (other.node2 != null)
				return false;
		} else if (!node2.equals(other.node2))
			return false;
		return true;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.node1 = in.readUTF();
		this.node2 = in.readUTF();
		this.edge = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(node1);
		out.writeUTF(node2);
		out.writeDouble(edge);
	}

	@Override
	public int compareTo(Edge node) {
		if (this.edge < node.edge)
			return 1;
		else {
			return 0;
		}
	}

}
