import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;

public class StandardDeviationDriver {

    public static void main(String[] args) throws Exception {
        /*
         * I have used my local path in windows change the path as per your
         * local machine
         */
        args = new String[] { "/home/vagrant/deviation/input",
                "/home/vagrant/deviation/output" };
        /* delete the output directory before running the job */
        FileUtils.deleteDirectory(new File(args[1]));
        /* set the hadoop system parameter */
        System.setProperty("hadoop.home.dir", "/usr/local/hadoop/bin");
        if (args.length != 2) {
            System.err.println("Please specify the input and output path");
            System.exit(-1);
        }
        Configuration conf = new org.apache.hadoop.conf.Configuration();
        // dodane
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );
        // dodane
        Job job = Job.getInstance(conf);
        job.setJarByClass(StandardDeviationDriver.class);
        job.setJobName("Find_Standard_Deviation");
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(StandardDeviationMapper.class);
        job.setReducerClass(StandardDeviationReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

