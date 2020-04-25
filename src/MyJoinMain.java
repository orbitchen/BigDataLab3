import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MyJoinMain {
    public static void main(String[] argv) throws Exception
    {
        Configuration conf=new Configuration();
        Job job=new Job(conf,"MyJoin");

        job.setJarByClass(MyJoinMain.class);

        job.setMapperClass(MyJoinMap.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(MyJoinReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        if(argv.length!=3)
        {
            System.err.println("Usage: InvertedIndex <in> <out>");
            System.exit(2);
        }

        FileInputFormat.addInputPath(job,new Path(argv[1]));
        FileOutputFormat.setOutputPath(job,new Path(argv[2]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
