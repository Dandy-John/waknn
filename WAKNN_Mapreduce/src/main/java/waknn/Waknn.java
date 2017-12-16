package waknn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import waknn.entity.Document;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public class Waknn {

    public static final int K = 3;

    /*
    输入key
    输入value：Document content (id, label, vector)
    输出key：Document id
    输出value：Document的预测分类
     */
    public static class KNNCalculateMapper extends Mapper<Object, Text, IntWritable, Text> {
        private Document[] documents;

        @Override
        protected void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            //读取configuration中存储的共享变量
            String documentsStr = configuration.get("documents");
            this.documents = Document.getDocumentArr(documentsStr);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            Document document = new Document(value.toString());
            Document[] others = new Document[documents.length - 1];
            boolean before = true;
            for (int i = 0; i < documents.length; ++i) {
                if (before && document.equals(documents[i])) {
                    before = false;
                    continue;
                }
                if (before) {
                    others[i] = documents[i];
                }
                else {
                    others[i - 1] = documents[i];
                }
            }
            Arrays.sort(others, document.getComparator());
            Map<String, Integer> K_neighbors = new HashMap<String, Integer>();
            for (int i = 0; i < K; ++i) {
                Document doc = others[i];
                if (K_neighbors.containsKey(doc.getLabel())) {
                    K_neighbors.put(doc.getLabel(), K_neighbors.get(doc.getLabel()) + 1);
                }
                else {
                    K_neighbors.put(doc.getLabel(), 1);
                }
            }
            String biggestLabel = null;
            int biggestCount = -1;
            for (String label : K_neighbors.keySet()) {
                if (biggestLabel == null) {
                    biggestLabel = label;
                    biggestCount = K_neighbors.get(label);
                }
                else if (K_neighbors.get(label) > biggestCount) {
                    biggestLabel = label;
                    biggestCount = K_neighbors.get(label);
                }
            }
            context.write(new IntWritable(document.getId()), new Text(biggestLabel));
        }
    }

    public static void main(String[] args) {
    }
}
