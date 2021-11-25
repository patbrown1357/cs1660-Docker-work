import java.io.IOException;
import java.util.StringTokenizer;

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
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public class WordCountMapper
        extends Mapper<Object, Text, Text, IntWritable> {
            public static final IntWritable one = new IntWritable(1);
            public static final Text word = new Text();
            public static final TreeMap<Text, Integer> wordCount = new TreeMap<Text, Integer>();
            public static final TreeMap<Integer, Text> invert = new TreeMap<Integer, Text>();

            public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
                    StringTokenizer itr = new StringTokenizer(value.toString());
                    while(itr.hasMoreTokens()) {
                        word.set(itr.nextToken());
                        wordCount.put(word, wordCount.get(word) + 1);
                    }

                    for(Text word: wordCount.keySet()) {
                        invert.put(wordCount.get(word), word);
                    }

                    for(Integer count: invert.descendingKeySet()) {
                        context.write(invert.get(count), count);
                    }
                }   
            }
        }

        public class WordCountReducer 
            extends Reducer<Text, IntWritable, Text, IntWritable> {

            public void reduce(Text key, Iterable<IntWritable> values, Context context)
                    throws IOException, InterruptedException {
                
                int sum = 0;
                for(IntWritable val : values) {
                    sum += val.get();
                }
                result.set(sum);
                context.write(key, result);
            }
        }
}