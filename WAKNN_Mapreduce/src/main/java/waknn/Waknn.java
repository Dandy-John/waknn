package waknn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import waknn.entity.Document;
import waknn.entity.Weight;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
//        private Document[] documents;
//        private Weight weight;

        @Override
        protected void setup(Context context) {
//            Configuration configuration = context.getConfiguration();
            //读取configuration中存储的共享变量
//            String documentsStr = configuration.get("documents");
//            this.documents = Document.getDocumentArr(documentsStr);
//            String weightStr = configuration.get("weight");
//            this.weight = new Weight(weightStr);
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
            Configuration conf = context.getConfiguration();
            String docsStr = conf.get("documents");
            Document[] docs = Document.getDocumentArr(docsStr);
            Weight weight = new Weight(conf.get("weight_attempt"));

            Document document = new Document(value.toString());

            Document[] others = new Document[docs.length - 1];
            boolean before = true;
            for (int i = 0; i < docs.length; ++i) {
                if (before && document.equals(docs[i])) {
                    before = false;
                    continue;
                }
                if (before) {
                    others[i] = docs[i];
                }
                else {
                    others[i - 1] = docs[i];
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
                conf.set("documents", Document.toString(docs.toArray()));
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

        Weight weight = Weight.init(100);
        double[] ratio = Weight.getRatio();
        conf.set("weight", weight.toParameter());
        conf.set("weight_attempt", weight.toParameter());

        Job job = jobInitialize(conf, "startup", otherArgs[0], otherArgs[1] + "-temp");
        System.exit(job.waitForCompletion(true) ?0 : 1);
        /*
        double baseAccuracy = getAccuracy();

        for (int i = 0; i < weight.getWeight().length; ++i) {
            mid:
            while (true) {
                for (double r : ratio) {
                    weight.setWeight(i, weight.getWeight(i) * r);
                    conf.set("weight_attempt", weight.toParameter());
                    //使用新的weight进行尝试，得到新的准确率
                    double accuracy = oneAttempt(baseAccuracy);
                    //若新的准确率高于原准确率，需要调整weight并重新进行所有ratio的尝试
                    if (accuracy > baseAccuracy) {
                        baseAccuracy = accuracy;
                        conf.set("weight", weight.toParameter());
                        continue mid;
                    }
                }
                break;
            }
        }
        */
        //TODO 求得了调整之后的权重，输出到文件

//        Job job = new Job(conf, "WAKNN");
//        job.setJarByClass(Waknn.class);
//        job.setMapperClass(Waknn.KNNCalculateMapper.class);
//        job.setReducerClass(Waknn.KNNCalculateReducer.class);
//        job.setOutputKeyClass(IntWritable.class);
//        job.setOutputValueClass(Text.class);
//        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
//        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
//        System.exit(job.waitForCompletion(true) ?0 : 1);
    }

    public static Job jobInitialize(Configuration conf, String jobName, String inArg, String outArg) throws IOException {
        Job job = new Job(conf, jobName);
        job.setJarByClass(Waknn.class);
        job.setMapperClass(Waknn.KNNCalculateMapper.class);
        job.setReducerClass(Waknn.KNNCalculateReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(inArg));
        FileOutputFormat.setOutputPath(job, new Path(outArg));
        return job;
    }

    public static double oneAttempt(double baseAccuracy) {
        //TODO 使用weight_attempt的权值对训练集中的所有元素进行一次预测，然后算出准确率
        return -1;
    }

    public static double getAccuracy(Path path, Configuration conf) throws IOException {
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream in = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));
        String docsStr = conf.get("documents");
        Document[] docs = Document.getDocumentArr(docsStr);
        String line = "";
        Map<Integer, String> predict = new HashMap<Integer, String>();
        while ((line = br.readLine()) != null) {
            String[] args = line.split(" ");
            int id = Integer.parseInt(args[0]);
            String label = args[1];
            predict.put(id, label);
        }
        int trueNumber = 0;
        for (Document doc : docs) {
            if (doc.getLabel().equals(predict.get(doc.getId()))) {
                trueNumber++;
            }
        }
        return ((double) trueNumber) / docs.length;
    }
}
