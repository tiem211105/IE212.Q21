import java.io.*;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GenderRatingAnalysis {

    public static class MyMapper extends Mapper<LongWritable, Text, Text, Text> {

        HashMap<String,String> userGender = new HashMap<>();
        HashMap<String,String> movieTitle = new HashMap<>();

        public void setup(Context context) throws IOException {

            BufferedReader br1 = new BufferedReader(new FileReader("users.txt"));
            String line;

            while((line = br1.readLine()) != null){
                String[] f = line.split(",\\s*");
                if(f.length >= 2){
                    userGender.put(f[0], f[1]);
                }
            }
            br1.close();

            BufferedReader br2 = new BufferedReader(new FileReader("movies.txt"));

            while((line = br2.readLine()) != null){
                String[] f = line.split(",\\s*",3);
                if(f.length >= 2){
                    movieTitle.put(f[0], f[1]);
                }
            }
            br2.close();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();
            String[] f = line.split(",\\s*");

            if(f.length >= 3){

                String userId = f[0];
                String movieId = f[1];
                String rating = f[2];

                String gender = userGender.get(userId);
                String title = movieTitle.get(movieId);

                if(gender != null && title != null){
                    context.write(new Text(title), new Text(gender + ":" + rating));
                }
            }
        }
    }

    public static class MyReducer extends Reducer<Text,Text,Text,Text>{

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            double maleSum = 0;
            double femaleSum = 0;

            int maleCount = 0;
            int femaleCount = 0;

            for(Text v : values){

                String[] p = v.toString().split(":");
                String gender = p[0];
                double rating = Double.parseDouble(p[1]);

                if(gender.equals("M")){
                    maleSum += rating;
                    maleCount++;
                }else if(gender.equals("F")){
                    femaleSum += rating;
                    femaleCount++;
                }
            }

            double maleAvg = maleCount==0 ? 0 : maleSum/maleCount;
            double femaleAvg = femaleCount==0 ? 0 : femaleSum/femaleCount;

            String result = String.format("Male: %.2f, Female: %.2f", maleAvg, femaleAvg);

            context.write(key, new Text(result));
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Gender Rating Analysis");

        job.setJarByClass(GenderRatingAnalysis.class);

        job.setMapperClass(MyMapper.class);
        job.setReducerClass(MyReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}



