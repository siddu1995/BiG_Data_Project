import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.Int;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.when;

public class rank {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL basic example")
                .config("spark.some.config.option", "some-value")
                .master("local")
                .getOrCreate();

        Dataset<Row> textFileRDD = spark.read().json("C:\\Users\\siddu_000\\Desktop\\Winter 2K18\\Big Data\\Assignment 2\\spark-demo\\dataIndices.txt");
        Dataset<Row> north = spark.read().option("delimiter","\t").csv("C:\\Users\\siddu_000\\Desktop\\Winter 2K18\\Big Data\\Assignment 2\\spark-demo\\joinedsjmr_north.tsv");

        List<Tuple2<String,Double>> qol = new ArrayList<>();
        List<Double> i = new ArrayList<>();
//        textFileRDD.foreach((ForeachFunction<Row>) row -> {
//            double cost_of_living_index=(Double.parseDouble(Double.toString(row.getAs("rent_index")))
//                    + Double.parseDouble(Double.toString(row.getAs("groceries_index")))
//                    + Double.parseDouble(Double.toString(row.getAs("restaurant_price_index"))));
//
//            double qol_index = Math.max(0, (100 + Double.parseDouble(Double.toString(row.getAs("purchasing_power_incl_rent_index"))) / 2.5
//                    - (Double.parseDouble(Double.toString(row.getAs("property_price_to_income_ratio"))) * 1.0)
//                    + Double.parseDouble(Double.toString(row.getAs("safety_index"))) / 2.0
//                    + Double.parseDouble(Double.toString(row.getAs("health_care_index"))) / 2.5
//                    - Double.parseDouble(Double.toString(row.getAs("traffic_time_index"))) / 2.0
//                    - Double.parseDouble(Double.toString(row.getAs("pollution_index"))) * 2.0 / 3.0
//                    + Double.parseDouble(Double.toString(row.getAs("climate_index"))) / 3.0
//                    - (cost_of_living_index / 10)));
//
//            String name = row.getAs("name");
//            i.add(qol_index);
//            System.out.println(i.size());
//            qol.add(new Tuple2<>(name,qol_index));
//            System.out.println(row.getAs("name")+" "+qol_index);
//        });

        StructType structType = new StructType();
        structType = structType.add("name", DataTypes.StringType, false);
        structType = structType.add("qol", DataTypes.DoubleType, false);

        ExpressionEncoder<Row> encoder = RowEncoder.apply(structType);
        Dataset<Row>s = textFileRDD.map((MapFunction<Row,Row>) row -> {
            double cost_of_living_index=(Double.parseDouble(Double.toString(row.getAs("rent_index")))
                    + Double.parseDouble(Double.toString(row.getAs("groceries_index")))
                    + Double.parseDouble(Double.toString(row.getAs("restaurant_price_index"))));

            double qol_index = Math.max(0, (100 + Double.parseDouble(Double.toString(row.getAs("purchasing_power_incl_rent_index"))) / 2.5
                    - (Double.parseDouble(Double.toString(row.getAs("property_price_to_income_ratio"))) * 1.0)
                    + Double.parseDouble(Double.toString(row.getAs("safety_index"))) / 2.0
                    + Double.parseDouble(Double.toString(row.getAs("health_care_index"))) / 2.5
                    - Double.parseDouble(Double.toString(row.getAs("traffic_time_index"))) / 2.0
                    - Double.parseDouble(Double.toString(row.getAs("pollution_index"))) * 2.0 / 3.0
                    + Double.parseDouble(Double.toString(row.getAs("climate_index"))) / 3.0
                    - (cost_of_living_index / 10)));

            String w = row.getAs("name");
            Row r = RowFactory.create(w,qol_index);
            return r;
        },encoder);

        Dataset<Row> comb = textFileRDD.join(s,"name").distinct();

        comb.show();
        Dataset<Row> filter1 = comb.select("name","qol");
        filter1.show();

        filter1.write()
                .format("com.databricks.spark.csv")
                .option("header", "false")
                .save("qol_final");

        Column newCol = when(col("_c1").like("%[tourism%"),1)
                .when(col("_c1").like("%[ameni%"),2)
                .when(col("_c1").like("%[lei%"),3);
        Dataset<Row> n = north.withColumn("_c1",newCol);
        Dataset<Row> grouped = n.groupBy(n.col("_c0"),n.col("_c1")).agg(sum(n.col("_c1")));

        Dataset<Row> osm = grouped.map((MapFunction<Row,Row>) row -> {
            int t_max = 313;
            int a_max = 8198;
            int l_max = 5877;

            int type = row.get(1) == null ? 0 : row.getInt(1);
            long count = row.get(2) == null ? 0 : row.getLong(2);
            double ind = 0.0;

            if(type == 1)
            {
                ind = (count/(double)t_max)*40;
            }
            else if(type == 2)
            {
                ind = (count/(double)t_max)*30;            }
            else if(type == 3)
            {
                ind = (count/(double)t_max)*30;            }
            else{
                                
            }

            String w = row.getAs("_c0");
            Row r = RowFactory.create(w,ind);
            return r;
        },encoder);

        osm.collect();
        System.out.println("Grouped index");

        Dataset<Row> comb_osm = osm.groupBy(osm.col("name")).agg(sum(osm.col("qol")));
        comb_osm.show();

        comb_osm.write()
                .format("com.databricks.spark.csv")
                .option("header", "false")
                .save("osm_final");
    }
}
