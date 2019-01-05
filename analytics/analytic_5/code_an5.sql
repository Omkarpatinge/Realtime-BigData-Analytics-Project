select age, count(*) as count from trafficTkts where age is not NULL group by age;

select gender, count(*) as count from trafficTkts where gender is not NULL group by gender;