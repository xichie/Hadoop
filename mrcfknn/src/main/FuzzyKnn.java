package main;

import java.util.ArrayList;

import util.Distance;
import util.OpUtils;

public class FuzzyKnn {
    public String labels[];
    public int k = 5; // k近邻参数
    public int m = 2; // 计算隶属度的参数


    public FuzzyKnn() {
    }

    public FuzzyKnn(int k, int m) {
        this.k = k;
        this.m = m;
    }

    /**
     * @param kNearestNeighbor testIns的k个近邻
     * @param data             测试样例
     * @return 测试样例的熵值
     * @throws Exception
     */
    public double run(ArrayList<Instance> kNearestNeighbor, Instance data)
            throws Exception {
        // 用算法A确定训练集的隶属度
        degree(kNearestNeighbor, labels);        //计算k个近邻的隶属度
        // 计算其类别隶属度
        Instance[] arr = new Instance[kNearestNeighbor.size()];
        for (int i = 0; i < arr.length; i++) {
            arr[i] = kNearestNeighbor.get(i);
        }
        data.u = getU(data, arr, m);
//		System.out.println(OpUtils.arrayToString(data.u));
        // 算出其信息熵
        Double entropy = entry(data);
        return entropy;

    }

    public void degree(ArrayList<Instance> trainSet, String[] c)
            throws Exception {

        Double[][] arr = new Double[trainSet.size()][c.length]; // 存储每个样例的类别隶属度
        Instance[] center = new Instance[c.length];
        for (int i = 0; i < c.length; i++) {
            center[i] = getCenter(trainSet, c[i]); // 获得每一类的中心,与c数组中的类别一一对应
        }

        Double[] dis = new Double[c.length]; // 存储样本到每一类别的距离

        for (int i = 0; i < trainSet.size(); i++) {

            for (int n = 0; n < center.length; n++) {
                // 计算样例到类别中心的距离，存储在dis中
                if (center[n] == null) {
                    dis[n] = 100000.0;
                } else {
                    dis[n] = Distance
                            .EuclideanDistance(trainSet.get(i)
                                    .getAtrributeValue(), center[n]
                                    .getAtrributeValue());
                }
            }

            Double sum = 0.0;
            for (int j = 0; j < dis.length; j++) {
                sum = sum + 1 / (dis[j] * dis[j] + 0.0001);
            }
            // 求出第i个样例j类的类别隶属度
            for (int j = 0; j < dis.length; j++) {
                arr[i][j] = 1 / (dis[j] * dis[j] + 0.0001) / sum;
            }
            // 将隶属度存储到对应样例的隶属度数组中
            trainSet.get(i).u = arr[i];
        }

    }

    //获得类别中心
    private Instance getCenter(ArrayList<Instance> trainSet, String lableValue) {
        double[] lable = new double[trainSet.get(0).getAtrributeValue().length];
        int num = 0;
        for (int i = 0; i < trainSet.size(); i++) {
            if (trainSet.get(i).getLable().equals(lableValue)) {
                for (int j = 0; j < lable.length; j++) {
                    lable[j] += trainSet.get(i).getAtrributeValue()[j];
                }
                num++;
            }

        }

        for (int i = 0; i < lable.length; i++) {
            if (num == 0) { // 如果没有该类，则中心为NULL
                return null;
            }
            lable[i] = lable[i] / num;
        }
        Instance center = new Instance(lable, lableValue);
        return center;
    }

    //计算样例的隶属度
    private Double[] getU(Instance instance, Instance[] kNerestInstance, int m)
            throws Exception {

        // 隶属度数组的大小
        Double[] u = new Double[labels.length];

        // 求测试样例的隶属度
        for (int j = 0; j < u.length; j++) {
            // 分子
            Double numerator = 0.0;
            // 分母
            Double denominator = 0.0;
            for (int i = 0; i < kNerestInstance.length; i++) {
                // 参数
                double uij = kNerestInstance[i].u[j];
                double dis = Distance.EuclideanDistance(
                        instance.getAtrributeValue(),
                        kNerestInstance[i].getAtrributeValue());
                if (dis == 0) {
                    dis = 0.00001;
                }
                numerator += uij * (1 / Math.pow(dis, 2 / (m - 1)));
                denominator += 1 / (Math.pow(dis, 2 / (m - 1)));

            }
            u[j] = numerator / denominator;
        }
        return u;
    }

    //计算样例的熵值
    private Double entry(Instance testIns) {
        Double entropy = 0.0;
        for (int i = 0; i < testIns.u.length; i++) {
            if (testIns.u[i] == 0) { // 如果隶属度为0，则让熵值为0（最小）
                return 0.0;
            }
            entropy += -(testIns.u[i] * (Math.log(testIns.u[i]) / Math.log(2)));
        }
        return entropy;

    }

}
