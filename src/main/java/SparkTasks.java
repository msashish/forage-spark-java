import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;


class SparkTasks {
    String input_file;
    SparkSession session;
    Dataset<Row> dataset;
    Utilities utilities = new Utilities();
    //Function<Row, Boolean> ForageFilter = row -> row.get(0) == "authorised" & row.get(1) != null & row.get(1).equals(0);

    SparkTasks(String input_file){
        this.input_file = input_file;
        this.session = SparkSession.builder().getOrCreate();
        this.dataset = ReadExcel();
    }

    Dataset<Row> ReadExcel() {
        return session.read()
                .format("com.crealytics.spark.excel")
                .option("header", "true")
                .load(this.input_file);
    }

    void ProcessDataFrame() {
        System.out.println("Executing DF API tasks for input " + this.input_file);

        long startTime = System.nanoTime();

        Dataset<Row> filtered_df = this.dataset
                .filter(this.dataset.col("status").equalTo("authorized"))
                .filter(this.dataset.col("card_present_flag").equalTo(0));
        Dataset<Row> long_lat_split_df = utilities.SplitColumns(
                filtered_df,
                "long_lat",
                "long",
                "lat",
                " "
        );
        Dataset<Row> merchant_long_lat_split_df = utilities.SplitColumns(
                long_lat_split_df,
                "merchant_long_lat",
                "merchant_long",
                "merchant_lat",
                " "
        );
        System.out.println("    Input data rows: before processing: " + this.dataset.count() +
                " and after processing: " + merchant_long_lat_split_df.count());

        utilities.WriteDSToCsv(merchant_long_lat_split_df, "output/abc_synth_df.csv");

        System.out.println("    Processing time taken by DataFrame API =  " +
                TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime));

    }

    void ProcessRDD() throws IOException {
        System.out.println("\nExecuting RDD API tasks for input " + this.input_file);

        long startTime = System.nanoTime();

        JavaRDD<Row> rdd = this.dataset.toJavaRDD();
        JavaRDD<Row> filtered_rdd = rdd
                .filter(row -> (Objects.equals(row.get(0), "authorized")))
                .filter(row -> (Objects.equals(row.get(1), "0")));

        JavaRDD<Row> split_rdd = filtered_rdd.map((Row row) -> RowFactory.create(row.mkString(","),
                row.get(5).toString().split(" ")[0],
                row.get(5).toString().split(" ")[1],
                row.get(21).toString().split(" ")[0],
                row.get(21).toString().split(" ")[1]
                )
        );
//        split_rdd.foreach(row -> {
//            System.out.println(row);
//        });

        System.out.println("    Input data rows: before processing: " + rdd.count() +
                " and after processing: " + split_rdd.count());

        utilities.WriteRDDToCsv(split_rdd,"output_rdd/abc_synth_rdd.csv");

        System.out.println("    Processing time taken by DataFrame API =  " +
                TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - startTime));

    }

}
