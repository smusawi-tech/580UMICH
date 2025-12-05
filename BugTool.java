import org.apache.hadoop.conf.Configuration; // options
import org.apache.hadoop.fs.Path; // paths
import org.apache.hadoop.io.IntWritable; // numbers
import org.apache.hadoop.io.Text; // words
import org.apache.hadoop.mapreduce.Job; // job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; // input
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat; // output
import org.apache.hadoop.io.LongWritable; // line num
import org.apache.hadoop.mapreduce.Mapper; // mapper base
import org.apache.hadoop.mapreduce.Reducer; // reducer base
import java.io.IOException; // errors
import java.util.regex.Pattern; // finder
import java.util.regex.Matcher; // helper
import java.util.TreeMap; // for sorting ranks
import java.util.HashSet; // for dupe clean

public class BugTool { // public main class
    public static void main(String[] args) throws Exception {
        System.out.println("Starting bug hunt!");
        try { // fixed: error handling
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "Bug Localizer");
            job.setJarByClass(BugTool.class); // this class
            job.setMapperClass(BugMapper.class); // mapper
            job.setReducerClass(BugReducer.class); // reducer
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            // input and output folder on HDFS
            FileInputFormat.addInputPath(job, new Path("input"));
            FileOutputFormat.setOutputPath(job, new Path("output"));
            boolean success = job.waitForCompletion(true);
            System.out.println("Done? " + success + " - processed " + job.getCounters().findCounter("org.apache.hadoop.mapred.Task$Counter", "MAP_INPUT_RECORDS").getValue() + " lines"); // fixed: metrics for test
            System.exit(success ? 0 : 1);
        } catch (Exception e) { // fixed: alternate flow error
            System.out.println("Big error: " + e.getMessage());
            System.exit(1);
        }
    }

  // Mapper class inside - fixed better cleaning (dupe skip, lowercase)
  public static class BugMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
      private final static IntWritable one = new IntWritable(1);
      private Text fileKey = new Text();
      private Pattern fileFinder = Pattern.compile("\\b[\\w.*-]+\\.(java|gif|class|html|properties|txt|csv|log|xml)\\b"); // fixed regex: reordered .*-
      private HashSet<String> seenLines = new HashSet<>(); // dupe clean

      public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
          String line = value.toString().trim().toLowerCase(); // fixed: lowercase for typo clean
          if (line.isEmpty() || line.startsWith("issue_id") || seenLines.contains(line)) {
              System.out.println("Skip junk or dupe!");
              return;
          }
          seenLines.add(line); // track dupe
          String[] parts = line.split("\t");
          if (parts.length < 6) {
              System.out.println("Short, skip!");
              return;
          }
          String desc = parts[5];
          System.out.println("Desc: " + desc.substring(0, Math.min(50, desc.length())) + "..."); // fixed: check length to avoid index error
          Matcher hunter = fileFinder.matcher(desc);
          while (hunter.find()) {
              String file = hunter.group();
              fileKey.set(file);
              context.write(fileKey, one);
              System.out.println("Found file: " + file);
          }
          if (!hunter.find()) {
              System.out.println("No files found in this line");
          }
      }
  }

  // Reducer class inside - fixed sort by count descending, CSV output
  public static class BugReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
      private TreeMap<Integer, Text> topFiles = new TreeMap<>(); // fixed: sort descending

      public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
          int total = 0;
          for (IntWritable v : values) {
              total += v.get();
          }
          topFiles.put(total * -1, new Text(key + "," + total)); // negative for reverse sort, CSV format
          System.out.println(key + " has " + total + " bugs");
      }

      protected void cleanup(Context context) throws IOException, InterruptedException {
          for (Text file : topFiles.values()) {
              context.write(file, null); // output sorted CSV
          }
          System.out.println("Exported sorted ranks as CSV in output");
      }
  }
  }