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

    public static String init(int size) {
        String[] word = new String[size];
        double[] weight = new double[size];
        for (int i = 0; i < size; ++i) {
            word[i] = "";
            weight[i] = random.nextDouble();
        }
        return new Weight(word, weight).toParameter();
    }

    public Weight(String paramStr) {
        String[] params = paramStr.split(" ");
        int length = params.length / 2;
        word = new String[length];
        weight = new double[length];
        for (int i = 0; i < length; ++i) {
            word[i] = params[i];
        }
        for (int i = length; i < params.length; ++i) {
            weight[i - length] = Double.parseDouble(params[i]);
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

    public void setWeight(double[] weight) {
        this.weight = weight;
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
