import java.io.IOException;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setNumReduceTasks(1);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class WordCountMapper
        extends Mapper<Object, Text, NullWritable, Text> {

            public final List<String> stopWord = Arrays.asList("he", "she", "they", "the", "a", "an", "are", "you", "of", "is", "and", "or");
            public final Text word = new Text();
            public final HashMap<String, Integer> wordCount = new HashMap<String, Integer>();
            public final TreeMap<Integer, String> topFive = new TreeMap<Integer, String>();

            public void map(Object key, Text value, Context context) 
                throws IOException, InterruptedException {
                    StringTokenizer itr = new StringTokenizer(value.toString());
                    while(itr.hasMoreTokens()) {
			            String token = itr.nextToken();
                        if(stopWord.contains(token)) {
                            continue;
                        }
                        if(wordCount.get(token) == null) {
                            wordCount.put(token, 0);
                        }
                        wordCount.put(token, wordCount.get(token) + 1);
                    }

                    for(String s: wordCount.keySet()) {
                        topFive.put(wordCount.get(s), s);
                        if( topFive.size() > 5) {
                            topFive.remove(topFive.firstKey());
                        }
                    }
            }

            @Override
            protected void cleanup(Context context) throws IOException, InterruptedException {
               for(Map.Entry<Integer, String> entry: topFive.entrySet()) {
                    context.write(NullWritable.get(), new Text(entry.getValue() + " " + entry.getKey()));
               }
            }

        }

        public static class WordCountReducer 
            extends Reducer<NullWritable, Text, Text, IntWritable> {
            
            public final HashMap<String, Integer> rankings = new HashMap<String, Integer>();
            public void reduce(NullWritable key, Iterable<Text> values, Context context)
                    throws IOException, InterruptedException {
                
                for(Text value: values) {
                    StringTokenizer itr = new StringTokenizer(value.toString());
                    String word = itr.nextToken();
                    int count = Integer.parseInt(itr.nextToken());
                    if(rankings.get(word) == null) {
                        rankings.put(word, 0);
                    }
		            int val = (int)rankings.get(word);
                    rankings.put(word, count + val);
                }

                TreeMap<Integer, String> invert = new TreeMap<Integer, String>();
                for(Object word: rankings.keySet()) {
		            String w2 = word.toString();
                    invert.put(rankings.get(w2), w2);
                }

                int i = 0;
                for(Object count: invert.descendingKeySet()) {
                    if(i < 5) {
			            int c2 = (int) count;
                        context.write(new Text(invert.get(c2)), new IntWritable(c2));
                        i++;
                    } else {
                        break;
                    }
                }
            }	
	}
}
