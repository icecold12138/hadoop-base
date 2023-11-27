create table test(id int,name string,gender string);
/*hive内部表又称管理表
extennal外部表，指表可以在任何位置，通过location指定位置，删除数据仅删除元数据，保留数据，删除数据信息，临时链接外部数据用*/
1.创建表
内部表
create database myhive;
use myhive;
create table stu(id int,name string);
insert into stu values(1,'周杰伦'),(2,'林俊杰');
select * from stu;
默认分隔符为\001，也可以建表的时候指定分隔符
row format delimited fields terminated by ‘\t’
create table stu2(id int,name string) row format delimited fields terminated by '\t';
insert into stu2 values(1,'周杰伦'),(2,'林俊杰');
外部表
表和数据是相互独立的
可以先有表，然后把数据移动到location指定目录中
create external table test_ext(id int,name string) row format delimited fields terminated by '\t' location '/tmp/test_ext1';
#指定到文件夹
select * from test_ext;
也可以现有数据，创建表时指定数据位置
create external table test_ext2(id int,name string) row format delimited fields terminated by '\t' location '/tmp/test_ext2';
select * from test_ext2;
drop table test_ext2;表删除了，但数据依旧存在
查看表
desc formatted test_ext;
desc formatted stu;
相互转化
alter table stu set tblproperties('EXTERNAL'='TRUE');
alter table test_ext set tblproperties('EXTERNAL'='FALSE');
load数据加载
create table test_load(
dt string comment '时间',
user_id string comment '用户ID',
search_word string comment '搜索关键字',
url string comment '网址') row format delimited fields terminated by '\t';
#本地加载
load data local inpath '/home/hadoop/search_log.txt' into table test_load;
load data local inpath '/home/hadoop/search_log.txt' overwrite into table test_load;
select * from test_load; 
hdfs加载，本质上是数据移动过程
load data inpath '/tmp/search_log.txt' into table test_load;
insert select从表向其他表加载
create table test_load2(
dt string comment '时间',
user_id string comment '用户ID',
search_word string comment '搜索关键字',
url string comment '网址') row format delimited fields terminated by '\t';
insert into test_load2 select * from test_load;
insert overwrite test_load2 select * from test_load;#数据覆盖
数据导出
insert overwrite local directory '/home/hadoop/export1' select * from test_load;
指定分隔符
insert overwrite local directory '/home/hadoop/export2' row format delimited fields terminated by '\t' select * from test_load;
导出到hdfs
insert overwrite directory '/tmp/export_to_hdfs' row format delimited fields terminated by '\t' select * from test_load;
方式2. /export/server/hive/bin/hive -e 'select * from myhive.test_load;' > /home/hadoop/export3.txt
/export/server/hive/bin/hive -f export.sql > /home/hadoop/export4.txt
分区表
把大的数据，按照每天，每个月的数据进行划分成小文件夹
create table score (id string,cid string,score int) partitioned by(month string) row format delimited fields terminated by '\t';
load data local inpath '/home/hadoop/score.txt' into table score partition(month='202005');
#创建多分区的表
create table score2(id string,cid string,score int) partitioned by(year string,month string,day string) row format delimited fields terminated by '\t';
load data local inpath '/home/hadoop/score.txt' into table score2 partition(year='2022',month='01',day='10');
分桶表
分桶是将表拆分到固定数量的不同文件中进行存储
创建分桶表
#开启分桶自动优化
set hive.enforce.bucketing=true;
create table course(c_id string,c_name string,t_id string) clustered by(c_id) into 3 buckets row format delimited fields terminated by '\t';
向分桶表加入数据,只能用insert SELECT 
需要基于分桶列进行hash取模计算，而load不会触发mapreduce，没有计算过程
创建临时表，用于向分桶表插入数据
create table course_common(c_id string,c_name string,t_id string) row format delimited fields terminated by '\t';
加载临时表数据
load data local inpath '/home/hadoop/course.txt' into table course_common; 
从中专表插入数据
insert into table course select * from course_common cluster by(c_id);
修改表属性
alter table score set TBLPROPERTIES('comment'='this is table comment');
alter table score set tblproperties('EXTERNAL'='TRUE')；
添加分区
alter table score2 add partition(year='2019',month='10',day='01');
修改分区(修改元数据记录，文件夹不会改名，但是在元数据记录中改名了)
alter table score2 partition(year='2022',month='01',day='10') rename to partition(year='2020',month='10',day='01');
删除分区(删除元数据，数据本身保留）)
alter table score2 drop partition(year='2020',month='10',day='01');
添加列
alter table score add columns(vi int,v2 string);
修改列名
alter table score change v2 v2new string;
删除表
drop table score;
清空表
truncate table course;
desc formatted test_ext;
alter table test_ext set TBLPROPERTIES('EXTERNAL'='TRUE');
truncate table test_ext;
#无法清空外部表
复杂类型array类型
create table test_array(name string,work_locations array<string>) row format delimited fields terminated by '\t' collection items terminated by ',';
load data local inpath '/home/hadoop/data_for_array_type.txt' into table test_array;
select name,work_locations[0] from test_array;
查询array元素个数
select name,size(work_locations) from test_array;
找找谁在天津工作过
select * from test_array where array_contains(work_locations,'tianjin');
map复杂类型，指代key，value型数据
create table test_map(id int,name string,members map<string,string>,age int) 
row format delimited fields terminated by ','
collection items terminated by '#'
map keys terminated by ':';
collection items terminated by '#'键值对之间的分隔符
map keys terminated by ':';单个键值对内部键和值之间的分隔符
load data local inpath '/home/hadoop/data_for_map_type.txt' into table test_map;
select * from test_map;
select name,members['father'],members['mother'] from test_map;
select map_keys(members) from test_map;
select map_values(members) from test_map;
显示键值对个数
select size(members) from test_map;
查看是否包含指定键
select * from test_map where array_contains(map_keys(members),'sister');
查看是否包含指定值
select * from test_map where array_contains(map_values(members),'王林');
struct复杂类型，是一个复合类型，可以在一个列中存入多个子列，每个子列允许设置类型和名称
create table test_struct(
id string,
info struct<name:string,age:int>)
row format delimited fields terminated by '#'
collection items terminated by ':';
load data local inpath '/home/hadoop/data_for_struct_type.txt' into table test_struct;
select id,info.name,info.age from test_struct;暂无可用函数
create database itheima;
use itheima;
drop database itheima;
CREATE DATABASE itheima;
USE itheima;
CREATE TABLE itheima.orders (
    orderId bigint COMMENT '订单id',
    orderNo string COMMENT '订单编号',
    shopId bigint COMMENT '门店id',
    userId bigint COMMENT '用户id',
    orderStatus tinyint COMMENT '订单状态 -3:用户拒收 -2:未付款的订单 -1：用户取消 0:待发货 1:配送中 2:用户确认收货',
    goodsMoney double COMMENT '商品金额',
    deliverMoney double COMMENT '运费',
    totalMoney double COMMENT '订单金额（包括运费）',
    realTotalMoney double COMMENT '实际订单金额（折扣后金额）',
    payType tinyint COMMENT '支付方式,0:未知;1:支付宝，2：微信;3、现金；4、其他',
    isPay tinyint COMMENT '是否支付 0:未支付 1:已支付',
    userName string COMMENT '收件人姓名',
    userAddress string COMMENT '收件人地址',
    userPhone string COMMENT '收件人电话',
    createTime timestamp COMMENT '下单时间',
    payTime timestamp COMMENT '支付时间',
    totalPayFee int COMMENT '总支付金额'
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INPATH '/home/hadoop/itheima_orders.txt' INTO TABLE itheima.orders;
创建用户表
CREATE TABLE itheima.users (
    userId int,
    loginName string,
    loginSecret int,
    loginPwd string,
    userSex tinyint,
    userName string,
    trueName string,
    brithday date,
    userPhoto string,
    userQQ string,
    userPhone string,
    userScore int,
    userTotalScore int,
    userFrom tinyint,
    userMoney double,
    lockMoney double,
    createTime timestamp,
    payPwd string,
    rechargeMoney double
) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';
LOAD DATA LOCAL INPATH '/home/hadoop/itheima_users.txt' INTO TABLE itheima.users;
rlike 正则表达式
查找广东省的数据
SELECT * FROM itheima.orders WHERE useraddress RLIKE '.*广东.*';
查找用户地址是：xx省 xx市 xx区的数据
select * from orders where useraddress rlike '..省 。。市 。。区';
查找用户姓为张、王、邓
select * from users where username rlike '^[张王邓]*.';
select * from users where username rlike '^[张王邓]\\S+';
查找手机号符合：188****0*** 规则
select * from orders where userphone rlike '188\\d{4}0\\d{3}';
select *from orders where userphone rlike '188\\S{4}0[0-9]{3}';
抽样表数据
语法1：随机分桶抽样,hash取模的值固定不变，执行抽样结果是固定不变的
select username,orderId from orders tablesample(bucket 3 out of 10 on username);
基于行抽样，每次运行结果不变
select * from orders tablesample(bucket 3 out of 10 on rand());
基于数据块抽样，每次结果都一致，条件不变，每一次抽样的结果都一致，从前到后按照顺序取取
抽取100条数据
select * from orders tablesample(100 rows);
抽取10%数据
select * from orders tablesample(10 percent) order by orderno desc;
抽取1k大小数据
select *from orders tablesample(1K);
虚拟列
set hive.exec.rowoffset=true;
input_file_name 显示数据行所在的具体文件
block_offset_inside_file 显示数据行所在文件的偏移量
row_offset_inside_block 显示数据所在hdfs块中的偏移量
use itheima;
select orderid,userid,input__file__name from orders;
函数
查看可用函数
show functions;
查看函数使用方法
describe function extended count;
求元素个数
use myhive;
select size(work_locations) from test_array;
select size(members) from test_map;
select *,sort_array(work_locations) from test_array;
类型转换
转二进制
select binary('hadoop');
自由转换
select cast('1' as bigint);
当前时间戳
select current_timestamp();
当前时间
select current_date();
select to_date(current_timestamp());
季度
select quarter('2020-05-04');
条件函数
nvl,如果value为null，则返回默认值，否则返回value
select nvl(null,0);
返回第一个不是null的v，如果都是null，返回null
select coalesce(truename,brithday) from itheima.users;
nullif(a,b)如果a=b，则返回null。否则返回a
数据脱敏函数
select mask_hash('hadoop');
hash加密
select hash('756');
md5加密
select md5('hadoop');

create database db_msg;
use db_msg;
create table db_msg.tb_msg_source(
msg_time string comment "消息发送时间",
sender_name string comment "发送人昵称",
sender_account string comment "发送人账号",
sender_sex string comment "发送人性别",
sender_ip string comment "发送人ip地址",
sender_os string comment "发送人操作系统",
sender_phonetype string comment "发送人手机型号",
sender_network string comment "发送人网络类型",
sender_gps string comment "发送人的GPS定位",
receiver_name string comment "接收人昵称",
receiver_ip string comment "接收人IP",
receiver_account string comment "接收人账号",
receiver_os string comment "接收人操作系统",
receiver_phonetype string comment "接收人手机型号",
receiver_network string comment "接收人网络类型",
receiver_gps string comment "接收人的GPS定位",
receiver_sex string comment "接收人性别",
msg_type string comment "消息类型",
distance string comment "双方距离",
message string comment "消息内容"
);
truncate table tb_msg_source;
load data inpath '/chatdemo/data/chat_data-30W.csv' into table tb_msg_source;
select * from tb_msg_source
limit 10;
数据清洗工作
select * from tb_msg_source where length(sender_gps)=0
需求
需求1：对字段为空的不合法数据进行过滤
select
from tb_msg_source
where length(sender_gps)>0;
where过滤
需求2：通过时间字段构建天和小时字段
select *,day(msg_time) as msg_day,hour(msg_time) as msg_hour from tb_msg_source;
date hour函数
需求3：从GPS的经纬度中提取经度和维度
select split(sender_gps,',')[0],split(sender_gps,',')[1] from tb_msg_source;
split函数
需求4：将ETL以后的结果保存到一张新的Hive表中
create table db_msg.tb_msg_etl(
msg_time string comment "消息发送时间",
sender_name string comment "发送人昵称",
sender_account string comment "发送人账号",
sender_sex string comment "发送人性别",
sender_ip string comment "发送人ip地址",
sender_os string comment "发送人操作系统",
sender_phonetype string comment "发送人手机型号",
sender_network string comment "发送人网络类型",
sender_gps string comment "发送人的GPS定位",
receiver_name string comment "接收人昵称",
receiver_ip string comment "接收人IP",
receiver_account string comment "接收人账号",
receiver_os string comment "接收人操作系统",
receiver_phonetype string comment "接收人手机型号",
receiver_network string comment "接收人网络类型",
receiver_gps string comment "接收人的GPS定位",
receiver_sex string comment "接收人性别",
msg_type string comment "消息类型",
distance string comment "双方距离",
message string comment "消息内容",
msg_day string comment "消息日",
msg_hour string comment "消息小时",
sender_lng double comment "经度",
sender_lat double comment "纬度"
);
insert overwrite table tb_msg_etl
select *,day(msg_time) as msg_day,hour(msg_time) as msg_hour,split(sender_gps,',')[0] as sender_lng,split(sender_gps,',')[1] as sender_lat from tb_msg_source tms where length(sender_gps)>0
案例分析
需求：
统计今日总消息量
create table tb_rs_total_msg_cnt comment '每日消息总量' as
select msg_day,count(*) as total_msg_cnt from tb_msg_etl group by msg_day; 

统计今日每小时消息量、发送和接收用户数
create table tb_rs_hour_msg_cnt comment '每小时消息量趋势' as
select msg_hour,count(*) as total_msg_cnt,count(distinct sender_account) as sender_user_cnt,count(DISTINCT receiver_account) as receiver_user_cnt from tb_msg_etl tme group by msg_hour;
统计今日各地区发送消息数据量
create table tb_rs_loc_cnt comment '今日各地区发送消息总量' as
select msg_day,sender_lng,sender_lat,count(*) as total_msg_cnt from tb_msg_etl group by msg_day,sender_lng,sender_lat
统计今日发送消息和接收消息的用户数
create table tb_rs_day_msg_cnt comment '今日发送和接受消息用户数' as
select msg_day,count(distinct sender_account) as sender_user_cnt,count(DISTINCT receiver_account) as receiver_user_cnt from tb_msg_etl tme group by msg_day
统计今日发送消息最多的Top10用户
create table tb_rs_sender_user_top10 comment '今日发送信息最多的10位用户' as
select sender_name,count(*) as sender_msg_cnt from tb_msg_etl group by sender_name order by sender_msg_cnt desc limit 10
统计今日接收消息最多的Top10用户
create table tb_rs_receiver_user_top10 comment '今日接受信息最多的10位用户' as
select receiver_name,count(*) as receiver_msg_cnt from tb_msg_etl group by receiver_name order by receiver_msg_cnt desc limit 10
统计发送人的手机型号分布情况
create table tb_rs_sender_phone comment '发送人手机型号分布情况' as
select sender_phonetype,count(*) as cnt from tb_msg_etl group by sender_phonetype
统计发送人的设备操作系统分布情况
create table tb_rs_sender_os comment '发送人设备操作系统分布情况' as
select sender_os,count(*) as cnt from tb_msg_etl group by sender_os
