import com.google.common.collect.Iterables;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class ChiSquare {

    // Applies the preprocessing and outputs either (category, term) or (term, category)
    public static class PreprocessingMapper extends Mapper<Object, Text, Text, Text> {
        private Text category = new Text();
        private Text term = new Text();
        private String delimiters = "\\s|\\d|\\.|!|\\?|,|;|:|\\(|\\)|\\[|]|\\{|}|-|_|\"|`|~|#|&|\\*|%|\\$|\\|/"; // delimiter regex as specified
        private Set<String> stopwords;
        private boolean categoryFirst;
        public static final Log log = LogFactory.getLog(PreprocessingMapper.class);

        public void setup(Context context) throws IOException {
            String[] stopwordsArray = context.getConfiguration().get("stopwords").split("\n");
            log.error(stopwordsArray);
            this.stopwords = new HashSet<>(Arrays.asList(stopwordsArray));
            this.stopwords.add("");
            this.categoryFirst = Boolean.parseBoolean(context.getConfiguration().get("categoryFirst"));
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject js = new JSONObject(value.toString());
            String reviewText = js.getString("reviewText");
            this.category.set(js.getString("category"));
            // Tokenization
            String[] unigrams = reviewText.split(delimiters);
            Set<String> tokens = new HashSet<>(Arrays.asList(unigrams));
            tokens.removeAll(this.stopwords); // stopword filtering
            for (String token : tokens) {
                token = token.toLowerCase(); // case folding
                this.term.set(token);
                if (categoryFirst) {
                    context.write(category, term);
                } else {
                    context.write(term, category);
                }
            }
        }
    }

    // Computes the chi square value by side joining with term and category occurences
    public static class MergeReducer extends Reducer<Text, Text, Text, Text> {
        public static final Log log = LogFactory.getLog(PreprocessingMapper.class);
        private Text termText = new Text();
        private long totalRecords;
        private Map<String, Term> termCounts = new HashMap<>();
        private Map<String, Integer> categoryCounts = new HashMap<>();

        public void setup(Context context) throws IOException, InterruptedException {
            this.totalRecords = Long.parseLong(context.getConfiguration().get("numDocuments"));

            URI[] cacheFileUris = context.getCacheFiles();
            // load all job results from fs (need to use directories due to num reducer
            for (int i = 0; i < 2; i++) {
                URI dirUri = cacheFileUris[i];
                Path dirPath = new Path(dirUri);
                RemoteIterator<LocatedFileStatus> files = FileSystem.get(context.getConfiguration()).listFiles(dirPath, false);
                while (files.hasNext()) {
                    LocatedFileStatus fileStatus = files.next();
                    Path path = fileStatus.getPath();
                    InputStream stream = FileSystem.get(context.getConfiguration()).open(path);
                    String contents = IOUtils.toString(stream);
                    for (String row : contents.split("\\n")) {
                        String[] cols = row.split("\\t");
                        if (cols.length < 2) {
                            continue;
                        }
                        if (i == 1) {
                            this.categoryCounts.put(cols[0], Integer.parseInt(cols[1]));
                        } else {
                            DatumReader<Term> termDatumReader = new SpecificDatumReader<>(Term.class);
                            Schema schema = Schema.parse(context.getConfiguration().get("termSchema"));
                            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, cols[1]);
                            termDatumReader.setSchema(schema);
                            log.info(cols[1]);
                            log.info(cols[1]);
                            Term term = termDatumReader.read(null, decoder);
                            this.termCounts.put(cols[0], term);
                        }
                    }
                }
            }
        }

        public void reduce(Text category, Iterable<Text> terms, Context context) throws IOException, InterruptedException {
            int categoryCount = categoryCounts.get(category.toString());
            Map<String, Integer> counts = new HashMap<>();
            for (Text term : terms) {
                int count = 1;
                String termString = term.toString();
                if (counts.containsKey(termString)) {
                    count += counts.get(termString);
                }
                counts.put(termString, count);
            }

            Map<String, Float> chiSquares = new HashMap<>();
            for (String term : counts.keySet()) {
                Term termDatum = this.termCounts.get(term);
                termDatum.setNumDocsInCategory(counts.get(term));
                termDatum.setNumDocsInCategoryWithout(categoryCount - counts.get(term));
                int A = termDatum.getNumDocsInCategory();
                int B = termDatum.getNumDocs() - termDatum.getNumDocsInCategory();
                int C = termDatum.getNumDocsInCategoryWithout();
                int D = termDatum.getNumDocsWithout() - termDatum.getNumDocsInCategoryWithout();
                float x2 = (float)((A * D - B * C) ^ 2) / ((A + B) * (A + C) * (B + D) * (C + D));
                chiSquares.put(term, x2);
            }

            String output = "";
            Object[] sortedChiSquares = MapUtil.sortByValue(chiSquares).keySet().toArray();
            for (int i = 0; i <= Math.min(200, sortedChiSquares.length); i++) {
                String term = sortedChiSquares[i].toString();
                String value = term + ":" + chiSquares.get(term);
                output += value + " ";
            }
            context.write(category, new Text(output));
        }
    }

    // Counts the overall occurence of a term in the corpus
    public static class TermReducer extends Reducer<Text, Text, Text, GenericRecord> {
        private int totalRecords;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            this.totalRecords = (int) currentJob.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        }


        public void reduce(Text term, Iterable<Text> categories, Context context) throws IOException, InterruptedException {
            Term termDatum = new Term();
            termDatum.setValue(term.toString());
            termDatum.setNumDocs(Iterables.size(categories));
            termDatum.setNumDocsWithout(totalRecords - termDatum.getNumDocs());
            context.write(term, termDatum);
        }
    }

    // Imply emits every category occurence
    public static class CategoryCountMapper extends Mapper<Object, Text, Text, IntWritable> {
        private Text category = new Text();
        private IntWritable ONE = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            JSONObject js = new JSONObject(value.toString());
            this.category.set(js.getString("category"));
            context.write(category, ONE);
        }
    }

    // Counts the overall occurence of a category in the corpus
    public static class CategoryCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        public static final Log log = LogFactory.getLog(PreprocessingMapper.class);
        private int totalRecords;

        public void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job currentJob = cluster.getJob(context.getJobID());
            //this.totalRecords = (int) currentJob.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue();
        }


        public void reduce(Text category, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(category, new IntWritable(Iterables.size(values)));
        }
    }

    public static void main(String[] args) {
        System.out.println("Test");
        String stopwords = "";  // load stopwords and term schema only once and pass it to jobs as config
        String termSchema = "";
        try {
            stopwords = new String(Files.readAllBytes(Paths.get(ChiSquare.class.getResource("stopwords.txt").toURI())));
            termSchema = new String(Files.readAllBytes(Paths.get(ChiSquare.class.getResource("term.avsc").toURI())));
        } catch (IOException | URISyntaxException e) {
            e.printStackTrace();
        }
        Configuration conf = new Configuration();
        conf.set("stopwords", stopwords);
        conf.set("categoryFirst", "false");
        conf.set("termSchema", termSchema);
        Path inputPath = new Path(args[0]);
        Path outputPath = new Path(args[1]);
        Path termOutputPath = new Path(outputPath, "term");
        Path categoryOutputPath = new Path(outputPath, "category");
        Path resultOutputPath = new Path(outputPath, "result");
        try {
            // first job: count the occurences of terms
            Job termJob = Job.getInstance(conf, "chi squared term");
            termJob.setJarByClass(ChiSquare.class);
            termJob.setNumReduceTasks(1000);
            termJob.setMapperClass(PreprocessingMapper.class);
            termJob.setReducerClass(TermReducer.class);
            termJob.setOutputKeyClass(Text.class);
            termJob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(termJob, inputPath);
            FileOutputFormat.setOutputPath(termJob, termOutputPath);
            termJob.submit(); // do not wait for completion

            // second job: count the occurences of categories
            Job categoryJob = Job.getInstance(conf, "chi squared term");
            categoryJob.setJarByClass(ChiSquare.class);
            categoryJob.setNumReduceTasks(23);
            categoryJob.setMapperClass(CategoryCountMapper.class);
            categoryJob.setReducerClass(CategoryCountReducer.class);
            categoryJob.setOutputKeyClass(Text.class);
            categoryJob.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(categoryJob, inputPath);
            FileOutputFormat.setOutputPath(categoryJob, categoryOutputPath);
            categoryJob.waitForCompletion(true);
            termJob.waitForCompletion(true); // jobs can be run parallel

            // last job: compute (category,term) occurences and side join  term and category occurences to compute X^2
            conf.set("categoryFirst", "true");
            conf.set("numDocuments", Long.toString(termJob.getCounters().findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue()));
            Job mergeJob = Job.getInstance(conf, "chi squared cat");
            mergeJob.addCacheFile(termOutputPath.toUri());
            mergeJob.setNumReduceTasks(23);
            mergeJob.addCacheFile(categoryOutputPath.toUri());
            mergeJob.setJarByClass(ChiSquare.class);
            mergeJob.setMapperClass(PreprocessingMapper.class);
            mergeJob.setReducerClass(MergeReducer.class);
            mergeJob.setOutputKeyClass(Text.class);
            mergeJob.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(mergeJob, inputPath);
            FileOutputFormat.setOutputPath(mergeJob, resultOutputPath);
            mergeJob.waitForCompletion(true);

            System.exit(0);
        } catch (IOException | ClassNotFoundException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
