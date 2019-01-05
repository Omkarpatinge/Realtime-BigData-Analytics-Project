import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TrafficTicketsIssuedProfilingMapper extends Mapper<LongWritable, Text, Text, Text>
{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

        String inputString = value.toString();
        String[] tokens = inputString.split(",");
        int colCount=9;

        int flag=1;

        StringBuffer sb = new StringBuffer("");
        for(int i=0; i<colCount;i++)
        {
                if(i==2 && (tokens[i].equals("2014") || tokens[i].equals("2015")))
                {
                        flag = 0;
                        break;
                }
                if(i==7 && (!tokens[i].contains("NEW YORK")))
                {
                        flag = 0;
                        break;
                }
                if(i==8 && !(tokens[i].contains("BRONX") || tokens[i].contains("MANHATTAN") || tokens[i].contains("QUEENS ") || tokens[i].contains("BROOKLYN") || tokens[i].contains("RICHMOND ")))
                {
                        flag = 0;
                        break;
                }
                sb.append(tokens[i]);
                sb.append(",");
        }

        sb.setLength(sb.length() - 1);
        if(flag != 0)
                context.write(new Text(), new Text(sb.toString()));
        }
}

