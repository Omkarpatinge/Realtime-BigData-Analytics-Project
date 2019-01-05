import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.text.*;
import java.util.*;

public class CollisionTimeMapper
extends Mapper<LongWritable, Text, Text,Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");
		if(!line[0].equals("DATE")){
			if(!(line[0].equals(""))) {
				StringBuilder sb= new StringBuilder();
				int flag=0;
				if(line[0].contains("-")){
					if(!line[0].split("-")[0].equals("0")){
	        				ArrayList<Date> al = new ArrayList<Date>();
	        				try{
	        					Date yourDate = new SimpleDateFormat("MM-dd-yyyy").parse(line[0]);
	        					al.add(yourDate);
	        					flag=1;
	        				}
	        				catch(ParseException e){

	        				}
	        				if(flag==1){
	        					Date yourDate = al.get(0);
		        				Calendar c = Calendar.getInstance();
		        				c.setTime(yourDate);
		        				int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
								sb.append(Integer.toString(dayOfWeek)+","+line[1].split(":")[0]);
								context.write(new Text(sb.toString()) , new Text(sb.toString()));
	        				}
					}
				}
				else if(line[0].contains("/")){
					if(!line[0].split("/")[0].equals("0")){
	        				ArrayList<Date> al = new ArrayList<Date>();
							try{
								Date yourDate = new SimpleDateFormat("MM/dd/yyyy").parse(line[0]);
								al.add(yourDate);
								flag=1;
							}
							catch (ParseException e) {
								
							}
							if(flag==1){
								Date yourDate=al.get(0);
		        				Calendar c = Calendar.getInstance();
		        				c.setTime(yourDate);
		        				int dayOfWeek = c.get(Calendar.DAY_OF_WEEK);
		    					sb.append(Integer.toString(dayOfWeek)+","+line[1].split(":")[0]);
								context.write(new Text(sb.toString()) , new Text(sb.toString()));
							}
					}
				}

			}
		}
	}
}	