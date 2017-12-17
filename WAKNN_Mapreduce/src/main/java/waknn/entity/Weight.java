package waknn.entity;

import java.util.Random;

public class Weight {
    private static Random random = new Random();
    private String[] word;
    private double[] weight;

    public Weight(String[] word, double[] weight) {
        this.word = word;
        this.weight = weight;
    }

    public static Weight init(int size) {
        String[] word = new String[size];
        double[] weight = new double[size];
        for (int i = 0; i < size; ++i) {
            word[i] = "";
            weight[i] = random.nextDouble();
        }
        return new Weight(word, weight);
    }

    public static double[] getRatio() {
        double[] result = {0.2, 0.8, 1.5, 2.0, 4.0};
        return result;
    }

    public Weight(String paramStr) {
        String[] params = paramStr.split(" ");
        word = new String[params.length];
        weight = new double[params.length];
        for (int i = 0; i < params.length; ++i) {
            word[i] = " ";
            weight[i] = Double.parseDouble(params[i]);
        }
    }

    public String[] getWord() {
        return word;
    }

    public void setWord(String[] word) {
        this.word = word;
    }

    public double[] getWeight() {
        return weight;
    }

    public double getWeight(int index) {
        return weight[index];
    }

    public void setWeight(double[] weight) {
        this.weight = weight;
    }

    public void setWeight(int index, double value) {
        weight[index] = value;
    }

    public String toParameter() {
//        String param = word[0];
//        for (int i = 1; i < word.length; ++i) {
//            param += " " + word[i];
//        }
        String param = String.valueOf(weight[0]);
        for (int i = 1; i < weight.length; ++i) {
            param += " " + String.valueOf(weight[i]);
        }
        return param;
    }
}
