package com.sentimentanalysis;


import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SentimentAnalysis {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokenizer = new StringTokenizer(value.toString(), ",");

            if (tokenizer.countTokens() == 6) {
                String target = tokenizer.nextToken().replaceAll("\"", "").trim();
                tokenizer.nextToken().replaceAll("\"", "").trim();
                tokenizer.nextToken().replaceAll("\"", "").trim();
                tokenizer.nextToken().replaceAll("\"", "").trim();
                tokenizer.nextToken().replaceAll("\"", "").trim();
                String text = tokenizer.nextToken().replaceAll("\"", "").trim();

                // Further processing if needed, e.g., removing mentions and punctuations
                String withoutMentionsAndPunctuations = removeMentionsAndPunctuations(text);

                // Create the sentiment label
                String emo = Integer.parseInt(target) == 0 ? "Negative" : "Positive";

                // Combine text, emotion label, and other information as needed
                text = target + "," + emo;

                Text valueText = new Text(text);
                word.set(withoutMentionsAndPunctuations);

                context.write(word, valueText);
            }
        }

        private static String removeMentionsAndPunctuations(String text) {
            // Remove mentions starting with '@'
            text = text.replaceAll("@\\w+", "");

            // Remove common punctuation
            text = text.replaceAll("[.,!?;:'\"()\\[\\]{}]", "");

            // Remove extra whitespaces
            text = text.replaceAll("\\s+", " ").trim();

            return text.toString().trim();
        }

    
    }

    public static void main(String[] args) throws Exception {
      Configuration conf = new Configuration();
      conf.set("mapreduce.output.textoutputformat.separator", ",");
      Job job = Job.getInstance(conf, "SentimentAnalysis");
      job.setJarByClass(SentimentAnalysis.class);
      job.setMapperClass(TokenizerMapper.class);
      job.setNumReduceTasks(0);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);  // Change to Text from IntWritable
      FileInputFormat.addInputPath(job, new Path(args[1]));
      FileOutputFormat.setOutputPath(job, new Path(args[2]));
      System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
  
}
