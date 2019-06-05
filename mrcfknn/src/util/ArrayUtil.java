package util;

import java.math.BigDecimal;

public class ArrayUtil {
	public static Double[] getDoubleArr(String[] arr) {
		Double[] result = new Double[arr.length];
		for (int i = 0; i < result.length; i++) {
			result[i] = Double.valueOf(arr[i]);
		}
		
		return result;
	}
	public static double formatDouble2(double d) {
		BigDecimal b = new BigDecimal(d);
		double f1 = b.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();

		return f1;
	}
	public static String toString(Double[][] arr) {
		String str = "";
		for (int i = 0; i < arr.length; i++) {
			for (int j = 0; j < arr[i].length; j++) {
				str = str + ArrayUtil.formatDouble2(arr[i][j]) + "  ";
			}
			str += "\r\n";
		}
		
		return str;
		
	}
}
