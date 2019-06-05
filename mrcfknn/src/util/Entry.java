package util;

//用来初始化隶属度矩阵，便于找到K个近邻
public class Entry {

	//	用来存储隶属度
	public double[] memship;
	//	用来存储距离
	public double distance;

	public Entry() {

	}

	public Entry(String str) {
		String temp = str.split(":")[1];
		String[] split = temp.split(";");
		memship = new double[split.length];
		for (int i = 0; i < split.length; i++) {
			String[] entryandlabel = split[i].split(",");
			int label = (int)Double.parseDouble(entryandlabel[1]);
			memship[label - 1] = Double.parseDouble(entryandlabel[0]);
		}
		distance = Double.parseDouble(str.split(":")[0]);
	}

}
