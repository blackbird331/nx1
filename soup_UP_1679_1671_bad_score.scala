import com.nexon.intelligencelab.cloudetl.CloudETLUtils._
import java.time.format.DateTimeFormatter                   
import java.time.LocalDateTime
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import scala.util.Try
import com.nexon.datainfra.util.BigQuery
import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.time.{LocalDate, Period}val zoneID  = "Asia/Seoul"
InitializeCloudEtlUtils2(spark, "dev-statistic-apps", "nxdb/db_info_sqlserver_v2.csv", "430011397", zoneID, "day")

val s3_bucket   = "live-nxlog-userprofile-ap-northeast-1"
val up_type     = "nxlog-custom"
val up_category = "account" 
val serviceID   = getServiceId
val up_dateType = if(getTerm == "D") "daily" else if(getTerm == "W") "weekly" else "monthly"
val up_table    = "estimated_yearage_daily"

val offsetDatetime = getOffsetDatetime  
// val offsetDatetime = LocalDateTime.parse("2020-06-30 00:00:00", DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))  // 임시로 시간 설정하기
val SetDateTime    = offsetDatetime.format(DateTimeFormatter.ofPattern("yyyyMMddHH")) // 시간 설정

setOffsetZonedDatetime(SetDateTime, zoneID)
printSettingTime

val sdt = getStartDatetime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))
val edt = getEndDatetime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))

val dateType  = getTerm                  // airflow에서 받아 올 날짜 타입. (D, W, M)
val dateStr   = getStartDatetime.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy/MM/dd"))
val sdate     = getStartDatetime.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
val sdate_minus_month = getStartDatetime.plusDays(1).plusMonths(-1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"))// 02-2 > for 7days daily - 01에서 쌓은 s3에서 가져옴
val sdateList = List[String]()

// val month_m0 = getEndDatetime.plusMonths(0).format(DateTimeFormatter.ofPattern("yyyy/MM"))
val month_m1 = getEndDatetime.plusMonths(-1).format(DateTimeFormatter.ofPattern("yyyy/MM/dd"))
val month_m2 = getEndDatetime.plusMonths(-2).format(DateTimeFormatter.ofPattern("yyyy/MM/dd"))
// val month_m3 = getEndDatetime.plusMonths(-3).format(DateTimeFormatter.ofPattern("yyyy/MM"))
// val month_m4 = getEndDatetime.plusMonths(-4).format(DateTimeFormatter.ofPattern("yyyy/MM"))
// val month_m5 = getEndDatetime.plusMonths(-5).format(DateTimeFormatter.ofPattern("yyyy/MM"))
// val month_m6 = getEndDatetime.plusMonths(-6).format(DateTimeFormatter.ofPattern("yyyy/MM"))
// val sdateList_ = month_m0 :: month_m1 :: month_m2 :: sdateListval query = s"""
            select *
            from ACCOUNT_PC.dbo.UP_BADWORDSCORE with (nolock)
            where sdate > '$month_m2'
            """  

val df = readDatabase(spark, query, "UP") 
                .withColumn("createdate", lit($"sdate"))
                .withColumn("sdate", lit(sdate))   
                .withColumn("loadingtime",from_utc_timestamp(current_timestamp(), "Asia/Seoul")) val df1 = df.filter($"nexonsn" > 0)
val df2 = df.filter($"npsn" > 0)// 데이터프레임 스키마의 nullable 속성 변경해주는 함수
def setNullableStateForAllColumns( df: DataFrame, columnMap: Map[String, Boolean]) : DataFrame = {

    // get schema
    val schema = df.schema
    val newSchema = StructType(schema.map {
    case StructField( c, d, n, m) =>
      StructField( c, d, columnMap.getOrElse(c, default = n), m)
    })
    // apply new schema
    df.sqlContext.createDataFrame( df.rdd, newSchema )
}


//필수컬럼 데이터타입 지정 필수.. 
val df_res1 = df1.withColumn("sdate",col("sdate").cast("timestamp"))                  
            .withColumn("timezone",col("timezone").cast("string"))
            .withColumn("accountno",col("accountno").cast("string"))
            .withColumn("nexonsn",col("nexonsn").cast("long"))
            .withColumn("npsn",col("npsn").cast("long"))
            .withColumn("accounttype",col("serviceareaid").cast("string"))  // serviceareaid -> accounttype으로 변경
            .withColumn("characterid",col("characterid").cast("string"))
            .withColumn("serviceid",col("serviceid").cast("long"))
            .withColumn("worldid",col("worldid").cast("string"))
            .withColumn("badwordscore", col("badwordscore").cast("double"))
            .withColumn("badwordcount", col("badwordcount").cast("long"))
            .withColumn("plainwordcount", col("plainwordcount").cast("long"))
            .withColumn("createdate", col("createdate").cast("date"))
            .withColumn("loadingtime",col("loadingtime").cast("timestamp"))
            .drop("serviceareaid")
            .selectExpr("sdate", "timezone", "accountno", "nexonsn", "npsn", "accounttype", "characterid", "serviceid", "worldid", "badwordscore", "badwordcount", "plainwordcount", "createdate", "loadingtime")

val df_res2 = df2.withColumn("sdate",col("sdate").cast("timestamp"))                  
            .withColumn("timezone",col("timezone").cast("string"))
            .withColumn("accountno",col("accountno").cast("string"))
            .withColumn("nexonsn",col("nexonsn").cast("long"))
            .withColumn("npsn",col("npsn").cast("long"))
            .withColumn("accounttype",col("serviceareaid").cast("string"))  // serviceareaid -> accounttype으로 변경
            .withColumn("characterid",col("characterid").cast("string"))
            .withColumn("serviceid",col("serviceid").cast("long"))
            .withColumn("worldid",col("worldid").cast("string"))
            .withColumn("badwordscore", col("badwordscore").cast("double"))
            .withColumn("badwordcount", col("badwordcount").cast("long"))
            .withColumn("plainwordcount", col("plainwordcount").cast("long"))
            .withColumn("createdate", col("createdate").cast("date"))
            .withColumn("loadingtime",col("loadingtime").cast("timestamp"))
            .drop("serviceareaid")
            .selectExpr("sdate", "timezone", "accountno", "nexonsn", "npsn", "accounttype", "characterid", "serviceid", "worldid", "badwordscore", "badwordcount", "plainwordcount", "createdate", "loadingtime")            

            
// UP 필수필드의 nullable = false 지정
val colNullableValues = Map("sdate" -> false,
                            "timezone" -> false,
                            "serviceid" -> false,
                            "nexonsn" -> false,
                            "npsn" -> false,
                            "accountno" -> false,
                            "accounttype" -> false,
                            "characterid" -> false,
                            "worldid" -> false,
                            "badwordscore" -> true,
                            "badwordcount" -> true,
                            "plainwordcount" -> true,
                            "createdate" -> true,
                            "loadingtime" -> false)
                            
val df_new1 = setNullableStateForAllColumns(df_res1, colNullableValues) 
val df_new2 = setNullableStateForAllColumns(df_res2, colNullableValues) 

// 빅쿼리 이름 세팅
val project = "c-mart-up"
val dataset = "UserProfile"
//val bigtable = "reportfilterconnect_accountno_daily_f1177_temp"
val bigtable1 = "badscore_nexonsn_f1679"
val bigtable2 = "badscore_npsn_f1701"
val sdatetime = getStartDatetime.plusHours(9).format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"))+ " UTC"
// 테이블 생성
val sql1 = s"""
CREATE TABLE IF NOT EXISTS `$project.$dataset.$bigtable1` 
(
    sdate timestamp not null
    , timezone string not null
    , accountno string not null 
    , nexonsn int64 not null 
    , npsn int64 not null
    , accounttype string not null 
    , characterid string not null 
    , serviceid int64 not null 
    , worldid string not null
    , badwordscore float64
    , badwordcount int64
    , plainwordcount int64
    , createdate date
    , loadingtime timestamp not null
)

PARTITION BY DATE(sdate)
CLUSTER BY sdate, nexonsn
OPTIONS(require_partition_filter=true);
"""
val sql2 = s"""
CREATE TABLE IF NOT EXISTS `$project.$dataset.$bigtable2` 
(
    sdate timestamp not null
    , timezone string not null
    , accountno string not null 
    , nexonsn int64 not null 
    , npsn int64 not null
    , accounttype string not null 
    , characterid string not null 
    , serviceid int64 not null 
    , worldid string not null
    , badwordscore float64
    , badwordcount int64
    , plainwordcount int64
    , createdate date
    , loadingtime timestamp not null
)

PARTITION BY DATE(sdate)
CLUSTER BY sdate, npsn
OPTIONS(require_partition_filter=true);
"""
 
BigQuery.waitFor("c-mart-up", sql1) 
BigQuery.waitFor("c-mart-up", sql2) 
  
  
// 데이터 중복방지 
val del_sql1 = s""" 
DELETE FROM `$project.$dataset.$bigtable1` 
WHERE sdate = '${sdatetime}' 
"""
  
val del_sql2 = s""" 
DELETE FROM `$project.$dataset.$bigtable2` 
WHERE sdate = '${sdatetime}' 
"""
  
BigQuery.waitFor("c-mart-up", del_sql1) 
BigQuery.waitFor("c-mart-up", del_sql2) 
  

// // 데이터 적재
BigQuery.write(df_new1, s"$project", s"$project:$dataset.$bigtable1") 
BigQuery.write(df_new2, s"$project", s"$project:$dataset.$bigtable2")
val featureId = 1679
val map_sdate = getStartDatetime.plusDays(1).format(DateTimeFormatter.ofPattern("yyyy-MM-dd")) // 데이터 적재 
val procedure_query = s"""call `c-mart-up.UserProfile.p_feature_sdate_map_table`($featureId, '$map_sdate');"""
BigQuery.waitFor("c-mart-up", procedure_query)

val featureId_ = 1701
val procedure_query_ = s"""call `c-mart-up.UserProfile.p_feature_sdate_map_table`($featureId_, '$map_sdate');"""
	BigQuery.waitFor("c-mart-up", procedure_query_)// import org.apache.spark.sql.types._
// import org.apache.spark.sql.Row
// val featureId = 1679

// val schema = StructType(List(
//     StructField("featureid", IntegerType, nullable = false),
//     StructField("sdatetime", TimestampType, nullable = true),
//     StructField("loadingtime", TimestampType, nullable = true)
// ))
// val input_val = sc.parallelize(Seq(
//   Row(featureId, null, null))
// )
// val feature_info = spark.createDataFrame(input_val, schema)
//                     .withColumn("sdatetime", lit(sdate).cast(TimestampType))
//                     .withColumn("loadingtime",from_utc_timestamp(current_timestamp(), "Asia/Seoul"))
                    
// BigQuery.waitFor(s"${project}", s"delete from UserProfile.feature_sdate_map_table where featureid=${featureId}")
// BigQuery.write(feature_info, s"${project}", "UserProfile.feature_sdate_map_table")// val featureId1 = 1701
  
// val schema1 = StructType(List( 
//     StructField("featureid", IntegerType, nullable = false), 
//     StructField("sdatetime", TimestampType, nullable = true), 
//     StructField("loadingtime", TimestampType, nullable = true) 
// )) 
// val input_val1 = sc.parallelize(Seq( 
//   Row(featureId1, null, null)) 
// ) 
// val feature_info1 = spark.createDataFrame(input_val1, schema1) 
//                     .withColumn("sdatetime", lit(sdate).cast(TimestampType)) 
//                     .withColumn("loadingtime",from_utc_timestamp(current_timestamp(), "Asia/Seoul")) 
// BigQuery.waitFor(s"${project}", s"delete from UserProfile.feature_sdate_map_table where featureid=${featureId1}") 
// BigQuery.write(feature_info1, s"${project}", "UserProfile.feature_sdate_map_table")