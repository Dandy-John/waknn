package waknn.entity;

import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

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

    public double distance(Document document, Weight weight) {
        if (this.equals(document)) {
            return 0;
        }

        // 用于计算距离的分子和分母
        double molecular = 0, denominator = 0, temp1 = 0, temp2 = 0;
        double [] array1 = document.getVector();
        double [] weightArray = weight.getWeight();
        for(int i = 0; i < array1.length; i++) {
            molecular += this.vector[i] * weightArray[i] * array1[i] * weightArray[i];
            temp1 += Math.pow(this.vector[i] * weightArray[i], 2);
            temp2 += Math.pow(array1[i] * weightArray[i], 2);
        }

        denominator = Math.pow(temp1, 0.5) * Math.pow(temp2, 0.5);

        if (denominator !=0 ) {
            return molecular / denominator;
        } else {
            return -1;
        }
    }

    public static Document[] getDocumentArr(String docsStr) {
        String[] docStr = docsStr.split("\n");
        Document[] documents = new Document[docStr.length];
        for (int i = 0; i < docStr.length; ++i) {
            documents[i] = new Document(docStr[i]);
        }



        return documents;
    }

    public static String toString(Object[] docs) {
        String str = docs[0].toString();
        for (int i = 1; i < docs.length; ++i) {
            str += "\n" + docs[i].toString();
        }
        return str;
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
        String str = String.valueOf(id) + " " + label;
        for (double v : vector) {
            str += " " + v;
        }
        return str;
    }

    public class DocumentComparator implements Comparator<Document> {
        private Document doc;
        private Weight weight;

        public DocumentComparator(Document doc, Weight weight) {
            this.doc = doc;
            this.weight = weight;
        }

        public int compare(Document o1, Document o2) {
            double dis1 = doc.distance(o1, weight);
            double dis2 = doc.distance(o2, weight);
            return -Double.compare(dis1, dis2);
        }
    }

    public DocumentComparator getComparator(Weight weight) {
        return new DocumentComparator(this, weight);
    }
}
