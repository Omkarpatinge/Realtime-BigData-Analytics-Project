import java.io.IOException;
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.LongWritable; 
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class DataETLMapper
extends Mapper<LongWritable, Text, Text, Text> {
        
        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {       
                String line = value.toString();
                String all_values[] = line.split(",");
                int cols_to_keep[] = {2, 4, 6, 7, 14, 19, 21, 23, 24, 26};  //Columns: Registration State, Issue Date, Vehicle Body Type, Vehicle Make, Violation Precinct, Violation Time, Violation County, House Number, Street Name, Date First Observed
                String values[] = new String[11];
                String ans = "";
                for(int i = 0; i<cols_to_keep.length; i++)
                {
                        values[i] = all_values[cols_to_keep[i]];
                        ans = ans+values[i]+",";
                }
                ans = ans.substring(0, ans.length()-1);
                context.write(new Text(ans), new Text(""));
        }
}
