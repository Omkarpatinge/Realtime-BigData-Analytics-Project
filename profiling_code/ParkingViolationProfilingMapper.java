import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ParkingViolationProfilingMapper
extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
/*Columns: Registration State, Issue Date, Vehicle Body Type, Vehicle Make, Violation Precinct, Violation Time, Violation County, House Number, Street Name, Date First Observed*/
                String line = value.toString();
                String values[] = line.split(",");
                String ans = "";
                int count_and_precinct_flag = 0;
                int flag = 1;
                String state, date, month, year, body_type, vehicle_make, precinct, violation_time, county, h_no, street_name, house_address;
                state = "";
                date = "";
                month = "";
                year = "";
                body_type = "";
                vehicle_make = "";
                precinct = "";
                violation_time = "";
                county = "";
                h_no = "";
                street_name ="";
                house_address = "";
                if(values[0].equals("NY"))
                {
                    state = "NY";
                }
                else
                {
                    flag = 0;
                }
                int date1 = 0;
                if(values[1].equals(""))
                {
                    date1 = 1;
                }
                else
                {
                    String d[] = values[1].split("/");
                    if(d.length!=3)
                    {
                        date1 = 1;
                    }
                    else
                    {
                        date = d[0].trim();
                        month = d[1].trim();
                        year = d[2].trim();
                        if(!(year.equals("2017") || year.equals("2016")))
                        {
                            date1 = 1;
                            year = "";
                            month = "";
                            date = "";
                        }
                        if(!(month.compareTo("01") >=0 && month.compareTo("12") <=0) )
                        {
                            date1 = 1;
                            year = "";
                            month = "";
                            date = "";
                        }
                        if(!(date.compareTo("01") >=0 && date.compareTo("31") <=0) )
                        {
                            date1 = 1;
                            year = "";
                            month = "";
                            date = "";
                        }
                        else
                        {
                            if(date.compareTo("28")>0 && (month.compareTo("02")==0))
                            {
                                date1 = 1;
                                year = "";
                                month = "";
                                date = "";
                            }
                            else if(date.compareTo("31")==0 && (month.compareTo("04")==0 || month.compareTo("06")==0 || month.compareTo("09")==0 || month.compareTo("11")==0))
                            {
                                date1 = 1;
                                year = "";
                                month = "";
                                date = "";
                            }
                        }
                    }
                }
                body_type = values[2];    
                vehicle_make = values[3];
                precinct = values[4];
                if(precinct.equals(""))
                {
                    count_and_precinct_flag++;
                }
                violation_time = values[5];
                if(violation_time.length()==5)
                {
                    if(violation_time.charAt(4)=='A')
                    {
                        violation_time = violation_time.substring(0, 2) + "00";
                    }
                    else
                    {
                        if(violation_time.charAt(0) >= '0' && violation_time.charAt(0) <= '9' && violation_time.charAt(1) >= '0' && violation_time.charAt(1) <= '9')
                        {
                            int time = Integer.parseInt(violation_time.substring(0, 2)) + 12;
                            violation_time = Integer.toString(time) + "00";        
                        }
                        else
                        {
                            violation_time = "";
                        }
                    }
                }
                county = values[6];
                if(county.equals(""))
                {
                    count_and_precinct_flag++;
                }
                if(county.contains("Q"))
                {
                    county = "Q";
                }
                else if(county.contains("K"))
                {
                    county = "K";
                }
                else if(county.contains("S")||county.contains("R"))
                {
                    county = "S";
                }
                else if(county.contains("NY") || county.contains("M"))
                {
                    county = "M";
                }
                else if(county.contains("B") )
                {
                    county = "B";
                }
                else
                {
                    count_and_precinct_flag ++;   
                }
                h_no = values[7];
                street_name = values[8];
                if((street_name.equals("")||h_no.equals("")) && count_and_precinct_flag==2)
                {
                    flag = 0;
                }
                else if(!(street_name.equals("")||h_no.equals("")))
                {
                    house_address = h_no.trim() + " " + street_name.trim();
                }
                int date2 = 0;
                if(values[9].equals(""))
                {
                    date2 = 1;
                }
                else if(!values[9].equals(""))
                {
                    values[9] = values[9].trim();
                    if(!date.equals("") && values[9].length()!=10)
                    {
                        date2 = 1;
                    }
                    else if(values[9].length()==10)
                    {
                        date = values[9].substring(6,8);
                        month = values[9].substring(4,6);
                        year = values[9].substring(0,4);
                        if(!(year.equals("2017") || year.equals("2016")))
                        {
                            date2 = 1;
                            date = "";
                            month = "";
                            year = "";
                        }
                        if(!(month.compareTo("01") >=0 && month.compareTo("12") <=0) )
                        {
                            date2 =1;
                            date = "";
                            month = "";
                            year = "";
                        }
                        if(!(date.compareTo("01") >=0 && date.compareTo("31") <=0) )
                        {
                            date2 =1;
                            date = "";
                            month = "";
                            year = "";
                        }
                        else
                        {
                            if(date.compareTo("28")>0 && (month.compareTo("02")==0))
                            {
                                date2 =1;
                                date = "";
                                month = "";
                                year = "";
                            }
                            else if(date.compareTo("31")==0 && (month.compareTo("04")==0 || month.compareTo("06")==0 || month.compareTo("09")==0 || month.compareTo("11")==0))
                            {
                                date2 =1;
                                date = "";
                                month = "";
                                year = "";
                            }
                        }
                    }
		    else
		    {
			date2 = 0;
		    }
                }
                if(date.equals(""))
                    flag = 0;
                if(flag != 0)
                {
                    ans = state + "," + date + "," + month + "," + year + "," + body_type + "," + vehicle_make + "," + precinct + "," + violation_time + "," + county + "," + house_address;
                    context.write(new Text(ans), new Text(""));
                }
        }
}

