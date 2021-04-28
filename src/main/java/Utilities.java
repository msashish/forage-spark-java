import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import static org.apache.spark.sql.functions.*;;

class Utilities {

    Dataset<Row> SplitColumns(Dataset<Row> df,
                              String column_name,
                              String new_column_name1,
                              String new_column_name2,
                              String delimiter) {
        return df.withColumn(new_column_name1, split(df.col(column_name), delimiter).getItem(0))
                .withColumn(new_column_name2, split(df.col(column_name), delimiter).getItem(1))
                .drop(column_name);

    }

    void WriteDSToCsv(Dataset<Row> df, String output_file){
        df.repartition(1)
                .write()
                .format("com.databricks.spark.csv")
                .mode("overwrite")
                .option("header", "true")
                .save(output_file);
    }

    void WriteRDDToCsv(JavaRDD<Row> rdd, String output_file) throws IOException {
        File output = new File(output_file);
        FileUtils.forceDelete(output);
        rdd.saveAsTextFile(output_file);
    }


}
