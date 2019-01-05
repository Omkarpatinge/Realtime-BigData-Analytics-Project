import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
public class Mapperclass
extends Mapper<LongWritable, Text, Text,Text> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] line = value.toString().split(",");
		if(!line[0].equals("DATE")){
			if(!(line[0].equals("")) && (!line[3].equals("") || (!line[4].equals("") && !line[5].equals("")) || !(line[2].equals("")))) {
				StringBuilder sb= new StringBuilder();
				if(!line[2].equals("")){
					if(line[2].equals("MANHATTAN")){
						line[2]="M";
					}
					else if(line[2].equals("BRONX")){
						line[2]="B";	
					}
					else if(line[2].equals("QUEENS")){
						line[2]="Q";
					}
					else if(line[2].equals("BROOKLYN")){
						line[2]="K";
					}
					else if(line[2].equals("STATEN ISLAND")){
						line[2]="S";
					}
				}

				if(line[0].contains("-")){
					if(!line[0].split("-")[0].equals("0")){
						sb.append(line[0].split("-")[0]+","+line[0].split("-")[1]+","+line[0].split("-")[2]+",");	
						if(!line[4].equals("") && !line[5].equals("")){
							if(Float.parseFloat(line[4])> 40.49749 && Float.parseFloat(line[4]) < 41.32691 && Float.parseFloat(line[5]) > -74.3912 && Float.parseFloat(line[5]) < -72.92314){
								sb.append(line[1]+","+line[2]+","+line[3]+","+line[4]+","+line[5]);						
							}
							else{
								sb.append(line[1]+","+line[2]+","+line[3]+","+",");
							}	
						}
						else{
							sb.append(line[1]+","+line[2]+","+line[3]+","+line[4]+","+line[5]);
						}
						context.write(new Text() , new Text(sb.toString()));
					}
				}
				else if(line[0].contains("/")){
					if(!line[0].split("/")[0].equals("0")){
						sb.append(line[0].split("/")[1]+","+line[0].split("/")[0]+","+line[0].split("/")[2]+",");
						if(!line[4].equals("") && !line[5].equals("")){
							if(Float.parseFloat(line[4])> 40.49749 && Float.parseFloat(line[4]) < 41.32691 && Float.parseFloat(line[5]) > -74.3912 && Float.parseFloat(line[5]) < -72.92314){
								sb.append(line[1]+","+line[2]+","+line[3]+","+line[4]+","+line[5]);						
							}
							else{
								sb.append(line[1]+","+line[2]+","+line[3]+","+",");
							}	
						}
						else{
							sb.append(line[1]+","+line[2]+","+line[3]+","+line[4]+","+line[5]);
						}
						context.write(new Text() , new Text(sb.toString()));
					}
				}


				
			}
		}
	}
}	