import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieRatingAverage {

    public static class MapClass extends Mapper<LongWritable, Text, IntWritable, FloatWritable> {

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] parts = line.split(",");

            if(parts.length >= 3){
                int movieID = Integer.parseInt(parts[1].trim());
                float rating = Float.parseFloat(parts[2].trim());

                context.write(new IntWritable(movieID), new FloatWritable(rating));
            }
        }
    }

    public static class ReduceClass extends Reducer<IntWritable, FloatWritable, Text, Text> {
	Map<Integer,String> movieNames = new HashMap<>();
	protected void setup(Context context) throws IOException {

    BufferedReader br = new BufferedReader(new FileReader("movies.txt"));
    String line;

    while((line = br.readLine()) != null){
        String[] parts = line.split(",",2);
        movieNames.put(Integer.parseInt(parts[0]), parts[1]);
    }

    br.close();
}
        float maxRating = 0;
        String maxMovie = "";

        public void reduce(IntWritable key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            float sum = 0;
            int count = 0;

            for(FloatWritable v : values){
                sum += v.get();
                count++;
            }

            float avg = sum / count;

            String movie = movieNames.get(key.get());

            context.write(new Text(movie),
                    new Text("Average rating: " + avg + " (Total ratings: " + count + ")"));

            if( avg > maxRating){
                maxRating = avg;
                maxMovie = movie;
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {

            context.write(new Text("RESULT"),
                    new Text(maxMovie + " is the highest rated movie with average rating " + maxRating));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Movie Rating Average");

        job.setJarByClass(MovieRatingAverage.class);

        job.setMapperClass(MapClass.class);
        job.setReducerClass(ReduceClass.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(FloatWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, MapClass.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MapClass.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
