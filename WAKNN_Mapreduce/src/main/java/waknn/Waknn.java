package waknn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import waknn.entity.Document;
import java.io.IOException;

public class Waknn {

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
            double[] distances = new double[documents.length];
            for (int i = 0; i < documents.length; ++i) {
                distances[i] = document.distance(documents[i]);
            }
        }
    }
}
