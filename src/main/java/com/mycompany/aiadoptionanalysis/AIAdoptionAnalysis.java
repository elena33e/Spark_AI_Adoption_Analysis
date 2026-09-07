/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */

package com.mycompany.aiadoptionanalysis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author elena
 */
public class AIAdoptionAnalysis {

    public static void main(String[] args) {
        // Initializare sesiune Spark 
        SparkSession spark = SparkConfig.getSession();

        try {
            System.out.println("Incepere procesare date");

            // Incarcare date
            Dataset<Row> companiesDF = DataLoader.loadCompanies(spark);
            Dataset<Row> countriesDF = DataLoader.loadReference(spark, "country_ai_index.csv");
            Dataset<Row> industriesDF = DataLoader.loadReference(spark, "ai_industry_summary.csv");

            // Exectare interogari           
            AnalysisService analysisService = new AnalysisService();
            analysisService.executeAnalysis(companiesDF, countriesDF, industriesDF);

            System.out.println("Rezultate salvate în folderul /results.");

        } catch (Exception e) {
            System.err.println(e.getMessage());
            e.printStackTrace();
        } finally {
            
            // Eliberarea resurselor clusterului
            if (spark != null) {
                spark.stop();
            }
        }
    }
}
