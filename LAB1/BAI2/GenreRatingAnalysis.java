import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class GenreRatingAnalysis {

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

            String[] parts = value.toString().split(",");

            if(parts.length >=3){
                String movieID = parts[0].trim();
                String genres = parts[2].trim();

                context.write(new Text(movieID), new Text("M|" + genres));
            }
        }
    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text>{

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException{

            String[] parts = value.toString().split(",");

            if(parts.length >=3){
                String movieID = parts[1].trim();
                String rating = parts[2].trim();

                context.write(new Text(movieID), new Text("R|" + rating));
            }
        }
    }

    public static class GenreReducer extends Reducer<Text, Text, Text, Text>{

        Map<String, Double> sumMap = new HashMap<>();
        Map<String, Integer> countMap = new HashMap<>();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException{

            String genres="";
            List<Double> ratings = new ArrayList<>();

            for(Text val : values){

                String v = val.toString();

                if(v.startsWith("M|")){
                    genres = v.substring(2);
		}
                else if(v.startsWith("R|")){
                    ratings.add(Double.parseDouble(v.substring(2)));
            }
	}

            if(!genres.equals("")){

                String[] genreList = genres.split("\\|");

                for(String g : genreList){
                    for(double r : ratings){

                        sumMap.put(g, sumMap.getOrDefault(g,0.0)+r);
                        countMap.put(g, countMap.getOrDefault(g,0)+1);
                    }
                }
            }
        }

        protected void cleanup(Context context) throws IOException, InterruptedException{

            for(String g : sumMap.keySet()){

                double avg = sumMap.get(g)/countMap.get(g);

                context.write(new Text(g),
                        new Text("Avg: "+String.format("%.2f",avg)+
                                ", Count: "+countMap.get(g)));
            }
        }
    }

    public static void main(String[] args) throws Exception{

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"Genre Rating");

        job.setJarByClass(GenreRatingAnalysis.class);

        MultipleInputs.addInputPath(job,new Path(args[0]),
                TextInputFormat.class,MovieMapper.class);

        MultipleInputs.addInputPath(job,new Path(args[1]),
                TextInputFormat.class,RatingMapper.class);

        job.setReducerClass(GenreReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job,new Path(args[2]));

        System.exit(job.waitForCompletion(true)?0:1);
    }
}
