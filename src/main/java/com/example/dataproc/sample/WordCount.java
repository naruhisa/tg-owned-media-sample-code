/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.example.dataproc.sample;

import java.io.IOException;
import java.util.StringTokenizer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.google.cloud.hadoop.io.bigquery.BigQueryFileFormat;
import com.google.cloud.hadoop.io.bigquery.output.BigQueryOutputConfiguration;
import com.google.cloud.hadoop.io.bigquery.output.IndirectBigQueryOutputFormat;
import com.google.cloud.hadoop.io.bigquery.BigQueryConfiguration;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import com.google.cloud.hadoop.io.bigquery.GsonBigQueryInputFormat;

import java.util.ArrayList;
import java.util.List;

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
      extends Reducer<Text,IntWritable,JsonObject,NullWritable> {

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;

      for (IntWritable val : values) {
        sum += val.get();
      }

      // 結果を格納した JsonObject を作成する。
      JsonObject jsonObject = new JsonObject();
      jsonObject.addProperty("Word", key.toString());
      jsonObject.addProperty("Count", sum);

      context.write(jsonObject, NullWritable.get());
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();

    // プロジェクトIDを設定する。
    String projectId = args[0];
    conf.set(BigQueryConfiguration.PROJECT_ID_KEY, projectId);

    // 出力用 Bigquery テーブルのidを設定
    // テーブル指定は ([projectId]:)[datasetId].[tableId] の形式//
    String outputQualifiedTableId = "wordcount_dataset.wordcount_table";
    String outputGcsPath = "gs://dataproc-bq-sample-2/jobs";

    // 入力に使用する BigQuery のテーブルを指定する。
    // テーブル指定は ([projectId]:)[datasetId].[tableId] の形式
    BigQueryConfiguration.configureBigQueryInput(conf, "bigquery-public-data:samples.github_nested");

    // BigQuery出力を設定する
    BigQueryOutputConfiguration.configureWithAutoSchema(
        conf,
        outputQualifiedTableId,
        outputGcsPath,
        BigQueryFileFormat.NEWLINE_DELIMITED_JSON,
        TextOutputFormat.class);

    Job job = Job.getInstance(conf, "word count");
    job.setJarByClass(WordCount.class);

    job.setMapperClass(TokenizerMapper.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    job.setInputFormatClass(GsonBigQueryInputFormat.class);
    job.setOutputFormatClass(IndirectBigQueryOutputFormat.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}