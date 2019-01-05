import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FinalTrafficTicketsIssuedMapper extends Mapper<LongWritable, Text, Text, Text>
{
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {

        String inputString = value.toString();
        String[] tokens = inputString.split(",");
        int colCount=0;
        String col[]=new String[10];
        int k=0;

        for(int i=0;i<=tokens.length-1;i++)
        {
                if(i!=7)
                {
                        col[k++]=tokens[i];
                        colCount++;
                }
        }

        StringBuffer sb = new StringBuffer("");
        for(int i=0; i<colCount;i++)
        {
                sb.append(col[i]);
                sb.append(",");
        }

        sb.setLength(sb.length() - 1);

        context.write(new Text(), new Text(sb.toString()));
        }
}
