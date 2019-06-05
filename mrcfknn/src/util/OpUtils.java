package util;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

import org.apache.hadoop.io.Text;

public class OpUtils {
	/*
	 * function：用来将字符串数组转换为字符串
	 * param0: 字符串数组
	 * return: 字符串
	 */
	public static String arrayToString(String[] s) {
		String str = "";
		for (int i = 0; i < s.length; i++) {
			if(i != s.length - 1)
				str += s[i]+",";
			else
				str += s[i];
		}
		return str;
	}
	/*
	 * function：用来将双精度数组转换为字符串数组
	 * param0: 双精度数组
	 * return: 字符串数组
	 */
	public static String arrayToString(Double[] s) {
		String str = "";
		for (int i = 0; i < s.length; i++) {
			if(i != s.length - 1)
				str += s[i]+",";
			else
				str += s[i];
		}
		return str;
	}

	/*
	 * function：用来计算样例的属性均值
	 * param0: 同一类样例的各属性的和
	 * param1: 样例个数
	 * return: 同类样例的属性均值
	 */
	public static double[] arrDivideNum(double[] sum, int num) {
		for (int i = 0; i < sum.length; i++) {
			sum[i] = sum[i] / num;
		}
		return sum;
	}

	/*
	 * function：用来计算两个样例的属性和
	 * param0: 样例属性值
	 * param1: 个数
	 * return: 样例属性和
	 */
	public static double[] arrayAdd(double[] attDouble, double[] sum) {
		for (int i = 0; i < sum.length; i++) {
			sum[i] = sum[i] + attDouble[i];
		}
		return sum;
	}

	/*
	 * function：用来将字符串转换为双精度数组
	 * param0: 字符串数组
	 * return: 双精度数组
	 */
	public static double[] arrayToDouble(String[] split) {
		double[] doublesplit = new double[split.length];
		for (int i = 0; i < split.length; i++) {
			doublesplit[i] = Double.parseDouble(split[i]);
		}
		return doublesplit;
	}
	/*
	 * function：用来找到K个近邻
	 * param0: 迭代器
	 * param1: 近邻个数
	 * return: K个近邻
	 */
	public static ArrayList<String> kNearest(Iterator<Text> iter, int k){
		ArrayList<String> list = new ArrayList<>();
		while(iter.hasNext()) {
			list.add(iter.next().toString());
		}
		// jdk1.8版本
		/*list.sort(new Comparator<String>() {

			@Override
			public int compare(String str1, String str2) {
				double dist1 = Double.parseDouble(str1.split(":")[0]);
				double dist2 = Double.parseDouble(str2.split(":")[0]);
				return (int) (dist1 - dist2);
			}
		});*/
		// jdk1.7
		Collections.sort(list, new Comparator<String>(){

			@Override
			public int compare(String str1, String str2) {
				double dist1 = Double.parseDouble(str1.split(":")[0]);
				double dist2 = Double.parseDouble(str2.split(":")[0]);
				return (int) (dist1 - dist2);
			}});

		ArrayList<String> kList = new ArrayList<>();
		for(int i = 0; i<k;i++) {
			kList.add(list.get(i));
		}

		return kList;


	}


}
