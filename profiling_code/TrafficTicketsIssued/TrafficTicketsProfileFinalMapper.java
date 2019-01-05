import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrafficTicketsProfileFinalMapper extends Mapper<LongWritable, Text, Text, Text>
{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

        String inputString = value.toString();
        String[] tokens = inputString.split(",");
        StringBuffer sb = new StringBuffer("");
        int colCount=8;
        for(int i=0; i<colCount;i++)
        {
                if(tokens[7].contains("BRONX"))
                {
                        tokens[7]="B";
                }
                else if(tokens[7].contains("MANHATTAN"))
                {
                        tokens[7]="M";
                }
                else if(tokens[7].contains("QUEENS "))
                {
                        tokens[7]="Q";
                }
                else if(tokens[7].contains("BROOKLYN"))
                {
                        tokens[7]="K";
                }
                else if(tokens[7].contains("RICHMOND "))
                {
                        tokens[7]="S";
                }
                        sb.append(tokens[i]);
                        sb.append(",");

        }

        sb.setLength(sb.length() - 1);
        context.write(new Text(), new Text(sb.toString()));
        }
}

