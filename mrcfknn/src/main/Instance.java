package main;

import util.ArrayUtil;

public class Instance {
    private double[] attributeValue;
    private String lable;
    public Double[] u;        //隶属度


    public Instance(String line) {
        String[] value = line.split("\\s+");            //分割符为空格
//		String[] value = line.split(","); 			//分隔符为逗号    与 toString对应
        attributeValue = new double[value.length - 1];

        //类别在第一列   与toString对应
        for (int i = 0; i < attributeValue.length; i++) {
            attributeValue[i] = Double.parseDouble(value[i + 1]);
        }
        lable = (int) Double.parseDouble(value[0]) + "";
        //类别在倒数一列
//		for (int i = 0; i < value.length - 1; i++) {
//			attributeValue[i] = Double.parseDouble(value[i]);
//		}
//		lable = (int)Double.parseDouble(value[value.length - 1]) + "";
    }

    public Instance(double[] attribute, String lableValue) {
        attributeValue = attribute;
        lable = lableValue;
    }

    public double[] getAtrributeValue() {
        return attributeValue;
    }

    public String getLable() {
        return lable;
    }


    @Override
    public String toString() {
        String value = "";
//		value += lable + ",";
//		for (int i = 0; i < attributeValue.length; i++) {
//			value += String.format("%.3f", attributeValue[i]) + ",";
//		}
        // 类别在第一列（默认空格分隔）
        value += lable + " ";
        for (int i = 0; i < attributeValue.length; i++) {
            value += String.format("%.3f", attributeValue[i]) + " ";
        }

//		value += lable;
        return value.trim();
    }

}
