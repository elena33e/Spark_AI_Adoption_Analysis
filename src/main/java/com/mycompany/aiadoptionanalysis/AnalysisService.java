/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.aiadoptionanalysis;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.storage.StorageLevel;

/**
 *
 * @author elena
 */
public class AnalysisService {

        private void saveAnalyticalResult(Dataset<Row> df, String folderName) {
                df.coalesce(1)
                                .write()
                                .mode(SaveMode.Overwrite)
                                .option("header", "true")
                                .csv("result/" + folderName);
                System.out.println("Rezultate salvate in: " + folderName);

        }

        public void executeAnalysis(Dataset<Row> companies, Dataset<Row> countries, Dataset<Row> industries) {
                SparkSession spark = companies.sparkSession();

                // Join cu countries + drop pe coloana region
                Dataset<Row> joinedWithCountries = companies
                                .join(broadcast(countries), "country")
                                .drop(countries.col("region"));
                // Join final
                Dataset<Row> fullData = joinedWithCountries
                                .join(broadcast(industries), "industry");

                fullData.persist(StorageLevel.MEMORY_ONLY());

                // INTEROGARI simple
                // 1. Companiile din Europa cu rata de adoptie peste 70%
                fullData.filter(col("region").equalTo("Europe").and(col("ai_adoption_rate").gt(70)))
                                .select("company_id", "country", "ai_adoption_rate")
                                .show(5);

                // 2. Top 10 după Scorul de Inovație
                fullData.orderBy(desc("innovation_score"))
                                .select("company_id", "industry", "innovation_score")
                                .show(10);

                // 3. Startup-uri cu buget AI mare
                fullData.filter("company_size = 'Startup' AND ai_budget_percentage > 15")
                                .select("company_id", "ai_budget_percentage")
                                .show(5);

                // 4. Unelte AI unice în Tehnologie
                fullData.filter(col("industry").equalTo("Technology"))
                                .select("ai_primary_tool").distinct()
                                .show();

                // 5. Top 5 țări după densitatea cercetătorilor
                fullData.select("country", "ai_researchers_per_million").distinct()
                                .orderBy(desc("ai_researchers_per_million"))
                                .show(5);

                // Interogari de analiza

                // Verificare dacă un buget mai mare aduce o creștere de venit mai mare,
                // eliminând companiile fără investiții
                fullData.select("industry", "ai_budget_percentage", "revenue_growth_percent")
                                .where(col("ai_budget_percentage").gt(5)) // Ne uităm doar la investiții serioase
                                .groupBy("industry")
                                .agg(avg("revenue_growth_percent").as("avg_growth_of_investors"))
                                .orderBy(desc("avg_growth_of_investors"))
                                .show();

                // 6. Calculează creșterea medie a productivității în funcție de politicile
                // naționale privind AI
                Dataset<Row> q6 = fullData.groupBy("country_ai_policy")
                                .agg(avg("productivity_change_percent").as("Avg_Productivity"));

                saveAnalyticalResult(q6, "productivitate_per_politica");

                // 7. Net job change per industrie
                Dataset<Row> q7 = fullData.groupBy("industry")
                                .agg(sum("jobs_created").as("Total_Created"), sum("jobs_displaced").as("Total_Lost"))
                                .withColumn("Net_Change", col("Total_Created").minus(col("Total_Lost")));
                saveAnalyticalResult(q7, "bilant_joburi_industrie");

                // 8. Stadiul adopției pe regiuni
                Dataset<Row> q8 = fullData.groupBy("region", "ai_adoption_stage").count();
                saveAnalyticalResult(q8, "distributie_stadii_adoptie");

                // 9. Venit mediu vs Maturitate Digitală (> 85)
                fullData.createOrReplaceTempView("ai_data");

                Dataset<Row> q9 = spark.sql(
                                "SELECT country, AVG(revenue_growth_percent) as Growth FROM ai_data "
                                                + "WHERE digital_maturity_index > 85 GROUP BY country");
                saveAnalyticalResult(q9, "crestere_venit_tari_mature");

                // 10. Outliers esec
                Dataset<Row> q10 = fullData.groupBy("company_id", "industry", "avg_ai_failure_rate")
                                .agg(avg("ai_failure_rate").as("avg_failure_multi_year"))
                                .filter(col("avg_failure_multi_year").gt(col("avg_ai_failure_rate")))
                                .orderBy(desc("avg_failure_multi_year"));

                saveAnalyticalResult(q10, "companii_outliers_esec");

                // 11. Top 3 inovatie pe regiune
                fullData.createOrReplaceTempView("ai_table");
                Dataset<Row> q11 = spark.sql(
                                "SELECT * FROM (SELECT region, company_id, innovation_score, " +
                                                "RANK() OVER (PARTITION BY region ORDER BY innovation_score DESC) as r FROM ai_table) "
                                                +
                                                "WHERE r <= 3");
                saveAnalyticalResult(q11, "top3_region");

                fullData.unpersist();

        }
}