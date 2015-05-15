/*
Copyright (c) 2015. Ramon de Vera Jr.
All Rights Reserved

Permission is hereby granted, free of charge, to any person obtaining a copy of
this software and associated documentation files (the "Software"), to deal in 
the Software without restriction, including without limitation the rights to use
, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of 
the Software, and to permit persons to whom the Software is furnished to do so,
subject to the following conditions:

The above copyright notice and this permission notice shall be included in all 
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR 
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR 
COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER 
IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN 
CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/
package com.github.sandgorgon.parmr;

import java.util.List;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroSchemaConverter;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileReader;
import parquet.schema.MessageType;

/**
 *
 * @author rdevera
 */
public class Main extends Configured implements Tool {

    public static final String jobName = "parmr";

    @Override
    public int run(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: parmr <input file> <output path>");
            return -1;
        }

        Configuration conf = super.getConf();
        conf.set("mapreduce.job.queuename", "prod");
        
        Job job = Job.getInstance(conf);
        job.setJobName(jobName);
        job.setJarByClass(Main.class);

        
        // Parquet Schema
        // Read from the input file itself the schema that we will be assuming
        Path infile = new Path(args[0]);
        List<Footer> footers = ParquetFileReader.readFooters(conf, infile.getFileSystem(conf).getFileStatus(infile), true);
        MessageType schema = footers.get(0).getParquetMetadata().getFileMetaData().getSchema();
        

        // Avro Schema
        // Convert the Parquet schema to an Avro schema
        AvroSchemaConverter avroSchemaConverter = new AvroSchemaConverter();
        Schema avroSchema = avroSchemaConverter.convert(schema);
        
        // Set the Mapper
        job.setMapperClass(UserMapper.class);
        
        // Set the Mapper
        
        // This works for predicate pushdown on record assembly read.
        AvroParquetInputFormat.setUnboundRecordFilter(job, UserRecordFilter.class);
        // But I don't know how to use the 2nd filter API, because I don't think it is just a matter of calling... ?!?
        //AvroParquetInputFormat.setFilterPredicate(conf, eq(intColumn("favorite_number"), 2));
        
        AvroParquetInputFormat.addInputPath(job, new Path(args[0]));
        AvroParquetInputFormat.setAvroReadSchema(job, avroSchema);
        job.setInputFormatClass(AvroParquetInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        
        
        // If you needed to return an avro object from the mapper, refer to this...
        //job.setMapOutputValueClass(AvroValue.class);
        //AvroJob.setMapOutputValueSchema(job, avroSchema);

        // Reducer
        job.setReducerClass(UserReducer.class);
        
        // Output
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        // If we need to return an avro class again, refer to this...
        //job.setOutputFormatClass(AvroParquetOutputFormat.class);
        //AvroParquetOutputFormat.setOutputPath(job, new Path(args[1]));
        //AvroParquetOutputFormat.setSchema(job, avroSchema);
        //job.setOutputKeyClass(Void.class);
        //job.setOutputValueClass(GenericRecord.class);

        // Rough way of testing the projection side of things.
        AvroParquetInputFormat.setRequestedProjection(job, Schema.parse(
                "{\"namespace\": \"com.github.sandgorgon.parmr.avro\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"User\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"name\", \"type\": \"string\"},\n" +
                "     {\"name\": \"favorite_number\",  \"type\": [\"int\", \"null\"]}\n" +
//                "     {\"name\": \"favorite_color\", \"type\": [\"string\", \"null\"]}\n" +
                " ]\n" +
                "}\n" +
                ""
        ));
        
        // Do the deed!
        int completion = job.waitForCompletion(true) ? 0 : 1;

        return completion;
    }

    public static void main(String[] args) throws Exception {
        // Let ToolRunner handle generic command-line options 
        int res = ToolRunner.run(new Configuration(), new Main(), args);

        System.exit(res);
    }
}
