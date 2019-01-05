import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WeatherProfileMapper
extends Mapper<LongWritable, Text, Text, Text> {
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {       
                String line = value.toString();
                String all_values[] = line.split(",");
                String values[] = new String[20];
                String ans = "";
                float snow_fall, prec, snow_dep;
                snow_fall = 0.00f;
                prec = 0.00f;
                snow_dep = 0.00f;
                String date[] = all_values[0].split("-");
                if(all_values[4].compareTo("T") ==0)
                {
                        all_values[4] = "1";       
                }
                else if(all_values[4].compareTo("") ==0)
                {
                        all_values[4] = "0";
                }
                else
                {
                        try {
                                prec = Float.parseFloat(all_values[4]);
                            }
                            catch (NumberFormatException e) {
                                prec = 0;
                            }
                }
                if(all_values[5].compareTo("T") ==0)
                {
                        all_values[5] = "1";       
                }
                else if(all_values[5].compareTo("") ==0)
                {
                        all_values[5] = "0";
                }
                else
                {
                        try {
                                snow_fall = Float.parseFloat(all_values[5]);
                            }
                            catch (NumberFormatException e) {
                                snow_fall = 0;
                            }
                }
                if(all_values[6].compareTo("T") ==0)
                {
                        all_values[6] = "1";       
                }
                else if(all_values[6].compareTo("") ==0)
                {
                        all_values[6] = "0";
                }
                else
                {
                        try {
                                snow_dep = Float.parseFloat(all_values[5]);
                            }
                            catch (NumberFormatException e) {
                                snow_dep = 0;
                            }
                }
                if(date.length == 3)
                {
                        ans = date[0]+","+date[1]+","+date[2]+","+Float.toString(prec) + ","+Float.toString(snow_fall)+ ","+Float.toString(snow_dep);
                        context.write(new Text(ans), new Text(""));        
                        
                }                

        }
}
