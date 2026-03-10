import java.io.*;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class AgeRatingAnalysis {

    public static class AgeMapper extends Mapper<LongWritable, Text, Text, Text> {

        private Map<String, String> userAgeGroup = new HashMap<>();
        private Map<String, String> movieTitles = new HashMap<>();

        protected void setup(Context context) throws IOException {

            BufferedReader userReader = new BufferedReader(new FileReader("users.txt"));
            String line;

            while ((line = userReader.readLine()) != null) {
                String[] parts = line.split(",");
                String userId = parts[0].trim();
                int age = Integer.parseInt(parts[2].trim());

                String group;

                if (age <= 18) group = "0-18";
                else if (age <= 35) group = "18-35";
                else if (age <= 50) group = "35-50";
                else group = "50+";

                userAgeGroup.put(userId, group);
            }
            userReader.close();


            BufferedReader movieReader = new BufferedReader(new FileReader("movies.txt"));

            while ((line = movieReader.readLine()) != null) {
                String[] parts = line.split(",");
                String movieId = parts[0].trim();
                String title = parts[1].trim();

                movieTitles.put(movieId, title);
            }
            movieReader.close();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",");

            if (parts.length < 3) return;

            String userId = parts[0].trim();
            String movieId = parts[1].trim();
            String rating = parts[2].trim();

            String ageGroup = userAgeGroup.get(userId);
            String movieTitle = movieTitles.get(movieId);

            if (ageGroup != null && movieTitle != null) {

                context.write(new Text(movieTitle),
                        new Text(ageGroup + ":" + rating));
            }
        }
    }


    public static class AgeReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Double> sum = new HashMap<>();
            Map<String, Integer> count = new HashMap<>();

            String[] groups = {"0-18", "18-35", "35-50", "50+"};

            for (String g : groups) {
                sum.put(g, 0.0);
                count.put(g, 0);
            }

            for (Text val : values) {

                String[] parts = val.toString().split(":");

                String group = parts[0];
                double rating = Double.parseDouble(parts[1]);

                sum.put(group, sum.get(group) + rating);
                count.put(group, count.get(group) + 1);
            }

            StringBuilder result = new StringBuilder();

            for (String g : groups) {

                if (count.get(g) == 0) {
                    result.append(g + ": NA ");
                } else {

                    double avg = sum.get(g) / count.get(g);
                    result.append(g + ": " + String.format("%.2f", avg) + " ");
                }
            }

            context.write(key, new Text(result.toString()));
        }
    }


    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Age Rating Analysis");

        job.setJarByClass(AgeRatingAnalysis.class);

        job.setMapperClass(AgeMapper.class);
        job.setReducerClass(AgeReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


