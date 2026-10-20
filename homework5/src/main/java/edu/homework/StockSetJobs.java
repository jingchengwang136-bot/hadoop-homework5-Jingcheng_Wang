package edu.homework;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/** Two sequential MapReduce jobs. Duplicate rows represent set membership only. */
public final class StockSetJobs extends Configured implements Tool {
    public enum Counters { INPUT_A, INPUT_B, EMPTY_LINES, UNIQUE_PAIRS }

    /** Numeric index order, followed by lexical stock-code order. */
    public static final class StockKey implements WritableComparable<StockKey> {
        private int index;
        private final Text stock = new Text();
        public StockKey() { }
        public StockKey(int index, String stock) { this.index = index; this.stock.set(stock); }
        @Override public void write(DataOutput out) throws IOException {
            out.writeInt(index); stock.write(out);
        }
        @Override public void readFields(DataInput in) throws IOException {
            index = in.readInt(); stock.readFields(in);
        }
        @Override public int compareTo(StockKey other) {
            int n = Integer.compare(index, other.index);
            return n == 0 ? stock.compareTo(other.stock) : n;
        }
        @Override public int hashCode() { return 31 * index + stock.hashCode(); }
        @Override public boolean equals(Object obj) {
            return obj instanceof StockKey && compareTo((StockKey) obj) == 0;
        }
        @Override public String toString() { return index + "\t" + stock; }
    }

    public abstract static class SourceMapper
            extends Mapper<LongWritable, Text, StockKey, IntWritable> {
        protected abstract int mask();
        @Override protected void map(LongWritable offset, Text value, Context ctx)
                throws IOException, InterruptedException {
            String line = value.toString().replace("\uFEFF", "").trim();
            if (line.isEmpty()) { ctx.getCounter(Counters.EMPTY_LINES).increment(1); return; }
            String[] fields = line.split("\\s+");
            if (fields.length != 2) throw new IOException("Expected 2 columns at offset " + offset);
            final int index;
            try { index = Integer.parseInt(fields[0]); }
            catch (NumberFormatException ex) { throw new IOException("Invalid index: " + fields[0], ex); }
            ctx.write(new StockKey(index, fields[1]), new IntWritable(mask()));
            ctx.getCounter(mask() == 1 ? Counters.INPUT_A : Counters.INPUT_B).increment(1);
        }
    }
    public static final class AMapper extends SourceMapper {
        @Override protected int mask() { return 1; }
    }
    public static final class BMapper extends SourceMapper {
        @Override protected int mask() { return 2; }
    }

    /** OR is associative, commutative and idempotent, so repeated records cannot inflate counts. */
    public static final class MaskCombiner
            extends Reducer<StockKey, IntWritable, StockKey, IntWritable> {
        @Override protected void reduce(StockKey key, Iterable<IntWritable> values, Context ctx)
                throws IOException, InterruptedException {
            int mask = 0;
            for (IntWritable value : values) mask |= value.get();
            ctx.write(key, new IntWritable(mask));
        }
    }
    public static final class RelationReducer
            extends Reducer<StockKey, IntWritable, StockKey, Text> {
        @Override protected void reduce(StockKey key, Iterable<IntWritable> values, Context ctx)
                throws IOException, InterruptedException {
            int mask = 0;
            for (IntWritable value : values) mask |= value.get();
            String tag;
            if (mask == 3) tag = "BOTH";
            else if (mask == 1) tag = "A_ONLY";
            else if (mask == 2) tag = "B_ONLY";
            else throw new IOException("Invalid membership mask: " + mask);
            ctx.write(key, new Text(tag));
            ctx.getCounter(Counters.UNIQUE_PAIRS).increment(1);
        }
    }

    public static final class StatsMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override protected void map(LongWritable offset, Text value, Context ctx)
                throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t", -1);
            if (fields.length != 3) throw new IOException("Invalid task1 output: " + value);
            ctx.write(new IntWritable(Integer.parseInt(fields[0])), new Text(fields[2]));
        }
    }
    public static final class StatsReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override protected void reduce(IntWritable index, Iterable<Text> tags, Context ctx)
                throws IOException, InterruptedException {
            long both = 0, onlyA = 0, onlyB = 0;
            for (Text tag : tags) {
                switch (tag.toString()) {
                    case "BOTH": both++; break;
                    case "A_ONLY": onlyA++; break;
                    case "B_ONLY": onlyB++; break;
                    default: throw new IOException("Invalid relation: " + tag);
                }
            }
            ctx.write(index, new Text(both + "\t" + onlyA + "\t" + onlyB + "\t" + (both + onlyA + onlyB)));
        }
    }

    @Override public int run(String[] args) throws Exception {
        if (args.length != 4) {
            System.err.println("Usage: StockSetJobs <A.txt> <B.txt> <output_task1> <output_task2>");
            return 2;
        }
        Path a = new Path(args[0]), b = new Path(args[1]);
        Path out1 = new Path(args[2]), out2 = new Path(args[3]);
        for (Path input : new Path[] {a, b}) {
            if (!input.getFileSystem(getConf()).isFile(input))
                throw new IOException("Input file not found: " + input);
        }
        for (Path output : new Path[] {out1, out2}) {
            FileSystem fs = output.getFileSystem(getConf());
            if (fs.exists(output)) throw new IOException("Output already exists; choose a new path: " + output);
        }
        String u = out1.getFileSystem(getConf()).makeQualified(out1).toString();
        String v = out2.getFileSystem(getConf()).makeQualified(out2).toString();
        if (u.equals(v) || u.startsWith(v + "/") || v.startsWith(u + "/"))
            throw new IOException("Output directories must be separate, not nested.");

        Job relations = Job.getInstance(getConf(), "hw5-task1-stock-relations");
        relations.setJarByClass(StockSetJobs.class);
        MultipleInputs.addInputPath(relations, a, TextInputFormat.class, AMapper.class);
        MultipleInputs.addInputPath(relations, b, TextInputFormat.class, BMapper.class);
        relations.setMapOutputKeyClass(StockKey.class);
        relations.setMapOutputValueClass(IntWritable.class);
        relations.setCombinerClass(MaskCombiner.class);
        relations.setReducerClass(RelationReducer.class);
        relations.setOutputKeyClass(StockKey.class);
        relations.setOutputValueClass(Text.class);
        relations.setNumReduceTasks(1); // A single output file provides global ordering.
        FileOutputFormat.setOutputPath(relations, out1);
        if (!relations.waitForCompletion(true)) return 1;

        Job stats = Job.getInstance(getConf(), "hw5-task2-index-counts");
        stats.setJarByClass(StockSetJobs.class);
        stats.setMapperClass(StatsMapper.class);
        stats.setMapOutputKeyClass(IntWritable.class);
        stats.setMapOutputValueClass(Text.class);
        stats.setReducerClass(StatsReducer.class);
        stats.setOutputKeyClass(IntWritable.class);
        stats.setOutputValueClass(Text.class);
        stats.setNumReduceTasks(1);
        FileInputFormat.addInputPath(stats, out1);
        FileOutputFormat.setOutputPath(stats, out2);
        return stats.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new StockSetJobs(), args));
    }
}
