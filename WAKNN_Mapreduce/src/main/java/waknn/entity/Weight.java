package waknn.entity;

public class Weight {
    private String[] word;
    private double[] weight;

    public Weight(String[] word, double[] weight) {
        this.word = word;
        this.weight = weight;
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
        String param = word[0];
        for (int i = 1; i < word.length; ++i) {
            param += " " + word[i];
        }
        for (int i = 0; i < weight.length; ++i) {
            param += " " + weight.toString();
        }
        return param;
    }
}
