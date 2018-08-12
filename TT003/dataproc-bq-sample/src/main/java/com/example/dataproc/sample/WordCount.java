package com.example.dataproc.sample;

import java.io.IOException;
import java.util.StringTokenizer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat;

public class WordCount {
  public static class TokenizerMapper
       extends Mapper<LongWritable, JsonObject, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, JsonObject value, Context context
                    ) throws IOException, InterruptedException {

      // "repository.name" を String として取得する
      JsonElement reposElement = value.get("repository");

      if (reposElement != null) {
        JsonObject reposObject = reposElement.getAsJsonObject();
        JsonElement nameElement = reposObject.get("name");

        if (nameElement != null) {
          String reposName = nameElement.getAsString();

          // "name" をトークナイズし、カウントする
          StringTokenizer itr = new StringTokenizer(reposName);
          while (itr.hasMoreTokens()) {
            word.set(itr.nextToken());
            context.write(word, one);
          }
        }
      }
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // プロジェクトIDを設定する。
    String projectId = args[0];
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);
    // 入力に使用する BigQuery のテーブルを指定する。
    // テーブル指定は [projectId]:[datasetId].[tableId] の形式
    BigQueryConfiguration.configureBigQueryInput(conf, "bigquery-public-data:samples.github_nested");

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setInputFormatClass(GsonBigQueryInputFormat.class);
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
