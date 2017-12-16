package waknn.entity;

public class Weight {
    private String[] word;
    private double[] weight;

    public Weight(String[] word, double[] weight) {
        if (Document.getLength() == -1) {
            throw new RuntimeException("length not set.");
        }
        if (word.length != Document.getLength() || weight.length != Document.getLength()) {
            throw new RuntimeException("length not fit.");
        }
        this.word = word;
        this.weight = weight;
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
}
