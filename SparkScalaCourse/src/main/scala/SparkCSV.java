import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Scanner;

public class SparkCSV {
        public static void main(String[] args) {
            SparkSession spark = SparkSession.builder()
                    .appName("Convert Transaction Time and Extract Tax ID")
                    .master("local[*]")
                    .getOrCreate();

            // Register UDF to fetch user name from API
            spark.udf().register("fetchUserName", (UDF1<Integer, String>) userId -> fetchUserName(userId), DataTypes.StringType);

            // Read CSV file
            Dataset<Row> df = spark.read()
                    .option("header", "true")
                    .option("inferSchema", "true")
                    .csv("data.csv");

            // Convert transaction_time column to date format and extract numbers from Tax_id
            Dataset<Row> transformedDf = df.withColumn("transaction_date",
                            functions.col("transaction_time").cast(DataTypes.DateType))

                    .withColumn("user_name",
                            functions.callUDF("fetchUserName", functions.col("user_id")))

                    .withColumn("extracted_tax_id",
                            functions.regexp_extract(functions.col("Tax_id"), "\\d+", 0));

//                    .withColumn("Capital name",functions.col("name").cast(DataTypes.StringType));

            // Show results
            transformedDf.show();

            // Write the transformed dataset to a new CSV file
            transformedDf.write()
                    .option("header", "true")
                    .csv("output.csv");

            // Stop Spark session
            spark.stop();
        }

        // Function to make API call to fetch user name
        public static String fetchUserName(Integer userId) {
            try {
//                URL url = new URL("https://api.restful-api.dev/objects/7?user=" + userId);
//                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
//                conn.setRequestMethod("GET");
//                conn.setRequestProperty("Accept", "application/json");
//
//                if (conn.getResponseCode() != 200) {
//                    return "Unknown";
//                }
//
//                Scanner scanner = new Scanner(conn.getInputStream());
//                StringBuilder response = new StringBuilder();
//                while (scanner.hasNext()) {
//                    response.append(scanner.nextLine());
//                }
//                scanner.close();
//                conn.disconnect();
//
//                // Extract user name from JSON response (assuming a simple format)
//                String responseString = response.toString();
                return "VENKATESH";
            } catch (Exception e) {
                return "Unknown";
            }
        }
    }

