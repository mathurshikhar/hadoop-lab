import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import codes.TemperatureAveragingPair;
public class MyAvgTemperature {
	//Mapper
	/**
	*AverageTemperatureMapper class is static and extends Mapper abstract class
	having four hadoop generics type LongWritable, Text, Text, Text.
	*/
		public static class AverageTemperatureMapper extends Mapper<LongWritable, Text, Text, Text> {
		////sample line of weather data
		private Text outText = new Text();
		@Override
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			//Converting the record (single line) to String and storing it in a String variable line
			String line = value.toString();
			if (!(line.length()== 0)) {
				//city identifier
				String wbanno = line.substring(0, 5);
				outText.set(wbanno);
				//Extracting Month - 01 02 03..
				//int Month = Integer.parseInt(line.substring(10, 12));
				 context.write(outText, new Text(value));
		}
	}
}
public static class AverageTemperaturePartitioner extends Partitioner <Text, Text>
 {
 public int getPartition (Text key, Text value,int numReduceTasks)
 {
 String line = value.toString();
 if(numReduceTasks==0)
 {
 return 0;
 }
 //Month
int Month = Integer.parseInt(line.substring(10, 12));
switch(Month){
case 1: return 1 % numReduceTasks;
case 2: return 2 % numReduceTasks;
case 3: return 3 % numReduceTasks;
case 4: return 4 % numReduceTasks;
case 5: return 5 % numReduceTasks;
case 6: return 6 % numReduceTasks;
case 7: return 7 % numReduceTasks;
case 8: return 8 % numReduceTasks;
case 9: return 9 % numReduceTasks;
case 10: return 10 % numReduceTasks;
case 11: return 11% numReduceTasks;
case 12: return 12 % numReduceTasks;
default : return 0;
}}}
 public static class AverageTemperatureReducer extends Reducer<Text, Text, Text, TemperatureAveragingPair> {
 private FloatWritable maxaverage = new FloatWritable();
 private FloatWritable minaverage = new FloatWritable();
 private FloatWritable pptaverage = new FloatWritable();
 private FloatWritable humiaverage = new FloatWritable();
 private FloatWritable soilmoisaverage = new FloatWritable();
 private FloatWritable soiltempaverage = new FloatWritable();
private TemperatureAveragingPair pair = new TemperatureAveragingPair();
 private static final int MISSING1 = 9999;
 private static final double MISSING2 =-9999.0;
 private static final double MISSING3 = -99.000;
 private static final double MISSING4 = 99.000;
 private static final double MISSING5 = 99.0;
 private static final double MISSING6 = 9999.0;
 @Override
 protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException,
InterruptedException {
 float tempmax = 0;
 float tempmin = 0;
 float precipitation =0;
 float humiavg =0;
 float soilmoisavg=0;
 float soiltempavg=0;
 int counttmax = 0;
 int counttmin = 0;
 int countppt =0;
 int counthumi=0;
 int countsoilmois=0;
 int countsoiltemp=0;
 for (Text val : values) {
 String line = val.toString();
 //max temperature in C
 float temp_Max = Float
.parseFloat(line.substring(39, 45).trim());
 //minimum temperaturein C
float temp_Min = Float.parseFloat(line.substring(47, 53).trim());
//Precipitation in mm
float precip =Float.parseFloat(line.substring(71,77).trim());
// Average relative humidity in %
float humi_avg = Float.parseFloat(line.substring(130,136).trim());
//Average soil moisture in fractional volumetric water content (m^3/m^3),
float soil_mois_avg = Float.parseFloat(line.substring(138,144).trim());
// Average soil temperature, in degrees C
float soil_temp_avg = Float.parseFloat(line.substring(178,184).trim());
 if (temp_Max != MISSING1) {
 tempmax += temp_Max;
 counttmax += 1;
 }
if (temp_Min != MISSING1) {
 tempmin += temp_Min;
 counttmin += 1;
 }
if( (precip!=MISSING1)&&(precip!=MISSING2)&&(precip!=MISSING3)&&
(precip!=MISSING4)&&(precip!=MISSING5)&&(precip!=MISSING6)){
precipitation+=precip;
countppt+=1;}
if((humi_avg!=MISSING3)&&(humi_avg!=MISSING4)&&(humi_avg!=MISSING1)&&(humi_avg!=MISSING2)&&(hum
i_avg!=MISSING6)){
humiavg+=humi_avg;
counthumi+=1;
}
if((soil_mois_avg!=MISSING3)&&(soil_mois_avg!=
MISSING4)&&(soil_mois_avg!= MISSING5)){
soilmoisavg+=soil_mois_avg;
countsoilmois+=1;
}
if((soil_temp_avg!=MISSING1)&&(soil_temp_avg!= MISSING2)&&(soil_temp_avg!=
MISSING6)){
soiltempavg+=soil_temp_avg;
countsoiltemp+=1;
}
 }
 maxaverage.set(tempmax / counttmax);
 minaverage.set(tempmin/ counttmin);
 pptaverage.set(precipitation/countppt);
 humiaverage.set(humiavg/counthumi);
 soilmoisaverage.set(soilmoisavg/countsoilmois);
 soiltempaverage.set(soiltempavg/countsoiltemp);
 pair.set(maxaverage,
inaverage,pptaverage,humiaverage,soilmoisaverage,soiltempaverage);
 context.write(key, pair);
 }
}
 /**
* @method main
* This method is used for setting all the configuration properties.
* It acts as a driver for map reduce code.
*/
public static void main(String[] args) throws Exception {
//reads the default configuration of cluster from the configuration xml files
Configuration conf = new Configuration();
//Initializing the job with the default configuration of the cluster
Job job = Job.getInstance(conf, "max mini temp avg example");
/Assigning the driver class name
job.setJarByClass(MyAvgTemperature.class);
//Key type coming out of mapper
job.setMapOutputKeyClass(Text.class);
//value type coming out of mapper
job.setMapOutputValueClass(Text.class);
//Defining the mapper class name
job.setMapperClass(AverageTemperatureMapper.class);
 //Defining the combiner class name
//job.setCombinerClass(AverageTemperatureCombiner.class);
//Defining the Partitioner Class name
job.setPartitionerClass(AverageTemperaturePartitioner.class);
job.setNumReduceTasks(12);
//Defining the reducer class name
job.setReducerClass(AverageTemperatureReducer.class);
//Defining input Format class which is responsible to parse the dataset into a key value pair
job.setInputFormatClass(TextInputFormat.class);
//Defining output Format class which is responsible to parse the dataset into a key value pair
job.setOutputFormatClass(TextOutputFormat.class);
//setting the second argument as a path in a path variable
//Path OutputPath = new Path(args[1]);
//Configuring the input path from the filesystem into the job
FileInputFormat.addInputPath(job, new Path(args[0]));
//Configuring the output path from the filesystem into the job
FileOutputFormat.setOutputPath(job, new Path(args[1]));
//deleting the context path automatically from hdfs so that we don't have delete it xplicitly
//OutputPath.getFileSystem(conf).delete(OutputPath);
//exiting the job only if the flag value becomes false
System.exit(job.waitForCompletion(true) ? 0 : 1);
}
}