import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

    public static void main(String[] args) throws Exception {
        if(args.length != 2) {
            System.err.println("Usage: WordCount <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setCombinerClass(WordCountReducer.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class WordCountMapper
        extends Mapper<Object, Text, Text, IntWritable> {

            public final List<String> stopWord = List.of("he", "she", "they", "the", "a", "an", "are", "you", "of", "is", "and", "or");
            public final IntWritable zero = new IntWritable(0);
            public final Text word = new Text();
            public final TreeMap<Text, Integer> wordCount = new TreeMap<Text, Integer>();
            public final TreeMap<Integer, Text> invert = new TreeMap<Integer, Text>();

            public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
                    StringTokenizer itr = new StringTokenizer(value.toString());
                    while(itr.hasMoreTokens()) {
                        if(stopWord.contains(itr.nextToken())) {
                            continue;
                        }
                        word.set(itr.nextToken());
                        if(wordCount.get(word) == null) {
                            wordCount.put(word, 0);
                        }
                        wordCount.put(word, wordCount.get(word) + 1);
                    }

                    for(Text word: wordCount.keySet()) {
                        invert.put(wordCount.get(word), word);
                    }
                    int i = 0;
                    for(Integer count: invert.descendingKeySet()) {
                        if(i < 5) {
                            context.write(NullWritable.get(), invert.get(count).append("-" + Integer.toString(count)));
                            i++;
                        } else {
                            break;
                        }
                    }
            }
        }

        public static class WordCountReducer 
            extends Reducer<Text, IntWritable, Text, IntWritable> {
            
            private Map rankings = new TreeMap<String, Integer>();
            public void reduce(NullWritable key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException {
                
                for(Text value: values) {
                    StringTokenizer itr = new StringTokenizer(value.toString(), "-");
                    String word = itr.nextToken();
                    int count = Integer.parseInt(itr.nextToken());
                    if(rankings.get(word) == null) {
                        rankings.put(word, 0);
                    }
                    rankings.put(word, count + rankings.get(word));
                }

                TreeMap<Integer, String> invert = new TreeMap<Integer, String>();
                for(Text word: rankings.keySet()) {
                    invert.put(wordCount.get(word), word);
                }

                int i = 0;
                for(Integer count: rankings.descendingKeySet()) {
                    if(i < 5) {
                        context.write(invert.get(count), new IntWritable(count));
                        i++;
                    } else {
                        break;
                    }
                }
            }	
	}
}
