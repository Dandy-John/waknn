package waknn.entity;

import java.util.Comparator;

public class Document {
    private int id;
    private String label;
    private double[] vector;

    public Document(String input) {

        String[] args = input.split(" ");
        this.id = Integer.parseInt(args[0]);
        this.label = args[1];
        int length = args.length - 2;
        this.vector = new double[length];
        for (int i = 2; i < length + 2; ++i) {
            this.vector[i - 2] = Double.parseDouble(args[i]);
        }
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

    public double distance(Document document) {
        if (this.equals(document)) {
            return 0;
        }
        //TODO complmete the cosin distance calculation
        return document.getId() - this.getId();
    }

    public static Document[] getDocumentArr(String docsStr) {
        String[] docStr = docsStr.split("\n");
        Document[] documents = new Document[docStr.length];
        for (int i = 0; i < docStr.length; ++i) {
            documents[i] = new Document(docStr[i]);
        }
        return documents;
    }

    @Override
    public boolean equals(Object object) {
        if (object instanceof Document) {
            Document document = (Document)object;
            return this.id == document.id;
        }
        return false;
    }

    @Override
    public String toString() {
        String str =  "Document {\n\tid = " + id + " label = " + label + "\n\tvector = [";
        for (double v : vector) {
            str += v + " ";
        }
        str += "]\n}";
        return str;
    }

    public class DocumentComparator implements Comparator<Document> {
        private Document doc;

        public DocumentComparator(Document doc) {
            this.doc = doc;
        }

        public int compare(Document o1, Document o2) {
            double dis1 = doc.distance(o1);
            double dis2 = doc.distance(o2);
            return Double.compare(dis1, dis2);
        }
    }

    public DocumentComparator getComparator() {
        return new DocumentComparator(this);
    }
}
