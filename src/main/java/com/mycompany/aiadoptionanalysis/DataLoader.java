package com.mycompany.aiadoptionanalysis;

import java.util.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;
import org.apache.spark.util.LongAccumulator;

public class DataLoader {

    public static Dataset<Row> loadCompanies(SparkSession spark) {
        // Definirea schemei
        String path = "data/ai_company_adoption.csv";

        // Identificăm coloane
        String[] cols = spark.read().option("header", "true").csv(path).columns();

        Map<String, DataType> types = new HashMap<>();
        types.put("ai_budget_percentage", DataTypes.DoubleType);
        types.put("ai_adoption_rate", DataTypes.DoubleType);
        types.put("innovation_score", DataTypes.DoubleType);
        types.put("productivity_change_percent", DataTypes.DoubleType);
        types.put("jobs_created", DataTypes.IntegerType);
        types.put("jobs_displaced", DataTypes.IntegerType);

        List<StructField> fields = new ArrayList<>();
        for (String c : cols) {
            fields.add(DataTypes.createStructField(c, types.getOrDefault(c, DataTypes.StringType), true));
        }
        StructType explicitSchema = DataTypes.createStructType(fields);
        
        // Citire inițială cu schema explicită
        Dataset<Row> rawDF = spark.read().schema(explicitSchema).option("header", "true").csv(path);

        System.out.println("Filrtrare RDD si acumulator");

        // Creare acummulator
        LongAccumulator zeroBudgetAcc = spark.sparkContext().longAccumulator("ZeroBudget");

        // Aplicăm filtrarea
        JavaRDD<Row> cleanedRDD = rawDF.toJavaRDD().filter(row -> {
            Double budget = row.getAs("ai_budget_percentage");

            if (budget != null && budget == 0.0) {
                zeroBudgetAcc.add(1);
                return false; // Eliminăm din setul de date
            }
            return true;
        });

        // Afisare randuri ramase
        System.out.println("Rânduri rămase după filtrare: " + cleanedRDD.count());
        System.out.println("Companii cu buget 0.0 identificate: " + zeroBudgetAcc.value());

        // Reconstruire dataframe
        return spark.createDataFrame(cleanedRDD, explicitSchema);
    }

    public static Dataset<Row> loadReference(SparkSession spark, String fileName) {
        return spark.read().option("header", "true").option("inferSchema", "true").csv("data/" + fileName);
    }
}