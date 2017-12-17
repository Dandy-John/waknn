package waknn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import waknn.entity.Document;
import waknn.entity.Weight;

import java.io.IOException;
import java.util.*;

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
        private Weight weight;

        @Override
        protected void setup(Context context) {
            Configuration configuration = context.getConfiguration();
            //读取configuration中存储的共享变量
            String documentsStr = configuration.get("documents");
            this.documents = Document.getDocumentArr(documentsStr);
            String weightStr = configuration.get("weight");
            this.weight = new Weight(weightStr);
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
            Arrays.sort(others, document.getComparator(weight));
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

        @Override
        public void run(Context context) throws IOException, InterruptedException {
            List<Document> docs = new ArrayList<Document>();
            List<Object> keys = new ArrayList<Object>();
            try {
                while (context.nextKeyValue()) {
                    Object key = context.getCurrentKey();
                    keys.add(key);
                    Text value = context.getCurrentValue();
                    docs.add(new Document(value.toString()));
                }

                Configuration conf = context.getConfiguration();
                conf.set("documents", Document.toString((Document[]) docs.toArray()));
                for (int i = 0; i < docs.size(); ++i) {
                    this.map(keys.get(i), new Text(docs.get(i).toString()), context);
                }
            } finally {
                this.cleanup(context);
            }
        }
    }

    public static class KNNCalculateReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            String value = null;
            for (Text val : values) {
                if (value == null) {
                    value = val.toString();
                }
                else if (!value.equals(val.toString())) {
                    throw new RuntimeException("reduce failed.");
                }
            }
            context.write(key, new Text(value));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs =new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length !=2) {
            System.exit(2);
        }
        String weight = "1 2 3 4 5 6";
        conf.set("weight", weight);

        Job job = new Job(conf, "WAKNN");
        job.setJarByClass(Waknn.class);
        job.setMapperClass(Waknn.KNNCalculateMapper.class);
        job.setReducerClass(Waknn.KNNCalculateReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ?0 : 1);
    }
}
