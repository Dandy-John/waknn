package waknn;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import waknn.entity.Document;
import waknn.entity.Weight;

import java.io.*;
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
        if (otherArgs.length !=3) {
            System.exit(2);
        }

        Weight weight = Weight.init(Integer.parseInt(otherArgs[3]));
        List<Document> documents = readDocuments(new Path(otherArgs[0]), conf);
        double[] ratio = Weight.getRatio();
        conf.set("weight", weight.toParameter());
        conf.set("weight_attempt", weight.toParameter());

        Job job = jobInitialize(conf, "startup", otherArgs[0], otherArgs[1] + "/temp-0");
        job.waitForCompletion(true);

        /*
        //测试：输出到文件
        Path outFile = new Path(otherArgs[1] + "-temp/out.txt");
        FileSystem fs = outFile.getFileSystem(conf);
        FSDataOutputStream out = fs.create(outFile);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, "utf-8"));
        bw.write("this is a line of text for test.");
        bw.close();

        System.exit(result ? 0 : 1);
        */
//        throw new Exception("documents value: " + conf.get("documents"));

//        double baseAccuracy = getAccuracy(new Path(otherArgs[1] + "/temp-0/part-r-00000"), conf, documents);
        double baseAccuracy = getAccuracy(new Path(otherArgs[1] + "/temp-0"), conf, documents);

        int count = 1;
        for (int i = 0; i < weight.getWeight().length; ++i) {
            mid:
            while (true) {
                for (double r : ratio) {
                    weight = new Weight(conf.get("weight"));
                    weight.setWeight(i, weight.getWeight(i) * r);
                    conf.set("weight_attempt", weight.toParameter());
                    //使用新的weight进行尝试，得到新的准确率
                    double accuracy = oneAttempt(count++, i, otherArgs[0], otherArgs[1], conf, documents);
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

        Path outFile = new Path(otherArgs[1] + "/result.txt");
        FileSystem fs = outFile.getFileSystem(conf);
        FSDataOutputStream out = fs.create(outFile);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(out, "utf-8"));
        bw.write("Final weight: " + conf.get("weight") + "\n");
        bw.write("Final acc: " + baseAccuracy + "\n");
        bw.close();


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

    public static double oneAttempt(int count, int i, String in, String out, Configuration conf, List<Document> documents) throws Exception {

        Job job = jobInitialize(conf, "mid-" + count, in, out + "/temp-" + count);
        job.waitForCompletion(true);
//        double accuracy = getAccuracy(new Path(out + "/temp-" + count + "/part-r-00000"), conf, documents);
        double accuracy = getAccuracy(new Path(out + "/temp-" + count), conf, documents);
        Path outFile = new Path(out + "/temp-" + count + "/result.txt");
        FileSystem fs = outFile.getFileSystem(conf);
        FSDataOutputStream outputStream = fs.create(outFile);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream, "utf-8"));
        bw.write("weight: " + conf.get("weight_attempt") + "\n");
        bw.write("acc: " + accuracy + "\n");
        bw.write("adjusting weight: " + i + "\n");
        bw.close();
        return accuracy;
    }

    public static double getAccuracy(Path path, Configuration conf, List<Document> documents) throws Exception {
        //TODO 当前传入的是一个文件，如果不止一个part-r-00000文件，则需要修改为读多个文件内容
        FileSystem fs = path.getFileSystem(conf);
        FileStatus[] fileStatuses = fs.listStatus(path);
        Path[] paths = FileUtil.stat2Paths(fileStatuses);

        Map<Integer, String> predict = new HashMap<Integer, String>();
        for (Path p : paths) {
            String name = p.getName();
            if (!name.startsWith("part-r-")) {
                continue;
            }
            FSDataInputStream in = fs.open(p);
            BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));
            String line = "";
            while ((line = br.readLine()) != null) {
                String[] args = line.split("\t");
                int id = Integer.parseInt(args[0]);
                String label = args[1];
                predict.put(id, label);
            }
        }
//        FSDataInputStream in = fs.open(path);
//        BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));
//        String line = "";
//        Map<Integer, String> predict = new HashMap<Integer, String>();
//        while ((line = br.readLine()) != null) {
//            String[] args = line.split("\t");
//            if (args.length < 2) {
//                throw new Exception(line);
//            }
//            int id = Integer.parseInt(args[0]);
//            String label = args[1];
//            predict.put(id, label);
//        }
        int trueNumber = 0;
        for (Document doc : documents) {
            if (doc.getLabel().equals(predict.get(doc.getId()))) {
                trueNumber++;
            }
        }
        return ((double) trueNumber) / documents.size();
    }

    public static List<Document> readDocuments(Path path, Configuration conf) throws IOException {
        List<Document> docs = new ArrayList<Document>();
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream in = fs.open(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(in, "utf-8"));
        String line;
        while ((line = br.readLine()) != null) {
            docs.add(new Document(line));
        }
        return docs;
    }
}
