import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
public class CollisionTimeReducer extends Reducer<Text, Text, Text, Text> {

public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	int finalValue=0;
	for (Text value : values) {
		finalValue++;
	}
	String daytime= key.toString();
	String day = daytime.split(",")[0];
    String hour= daytime.split(",")[1];
    StringBuilder sb = new StringBuilder();
    switch(day){
        case "1":
            sb.append("Sunday"+",");
            break;
        case "2":
            sb.append("Monday"+",");
            break;
        case "3":
            sb.append("Tuesday"+",");
            break;
        case "4":
            sb.append("Wednesday"+",");
            break;
        case "5":
            sb.append("Thursday"+",");
            break;
        case "6":
            sb.append("Friday"+",");
            break;
        case "7":
            sb.append("Saturday"+",");
            break;
    }
    sb.append(hour+","+Integer.toString(finalValue));
	context.write(new Text("Count,"), new Text(sb.toString()));
	}
}