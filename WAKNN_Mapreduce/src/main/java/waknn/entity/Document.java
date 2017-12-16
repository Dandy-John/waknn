package waknn.entity;

public class Document {
    private static int length = -1;
    private int id;
    private String label;
    private double[] vector;

    public Document(String input) {
        if (Document.length == -1) {
            throw new RuntimeException("length not set.");
        }

        String[] args = input.split(" ");
        this.id = Integer.parseInt(args[0]);
        this.label = args[1];
        this.vector = new double[length];
        for (int i = 2; i < length + 2; ++i) {
            this.vector[i - 2] = Double.parseDouble(args[i]);
        }
    }

    public static void setLength(int length) {
        Document.length = length;
    }

    public static int getLength() {
        return Document.length;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getLabel() {
        return label;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public double[] getVector() {
        return vector;
    }

    public void setVector(double[] vector) {
        this.vector = vector;
    }
}
