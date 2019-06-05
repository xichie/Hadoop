package main;

import util.Distance;

import java.util.ArrayList;

public class Knn {

	public Instance[] fun(Instance testIns, ArrayList<Instance> trainSet, int k )
			throws Exception {
		//在训练集中找到k个近邻
		Instance[] insArr = findKNN(testIns,trainSet,k);
		return insArr;
	}

	public static Instance[] findKNN(Instance ins, ArrayList<Instance> trainSet, int k) throws Exception {
		//最近的k个样例
		Instance[] arr = new Instance[k];
		//对应最近的k个样例的距离
		Double[] distance = new Double[k];
		//遍历训练集，找到最近的k个样例
		int num = 0;
		for(Instance trainIns : trainSet){
			double dis = Distance.EuclideanDistance(ins.getAtrributeValue(), trainIns.getAtrributeValue());
//			if(dis == 0.0){		// 两个样例相同， 没有必要进行计算。
//				return null;
//			}
			if(num < distance.length) {
				distance[num] = dis;
				arr[num] = trainIns;
				num++;
			}else {
				int index = getMaxIndex(distance);
				if(dis < distance[index]) {
					distance[index] = dis;
					arr[index] = trainIns;
				}
			}

		}

		return arr;
	}
	public static int getMaxIndex(Double[] distance) {
		double max = 0;
		int index = 0;
		for (int i = 0; i < distance.length; i++) {
			if(distance[i]!=null) {
				if(distance[i] > max) {
					max = distance[i];
					index = i;
				}
			}
		}
		return index;
	}
}
