/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.aiadoptionanalysis;
import org.apache.spark.sql.SparkSession;
/**
 *
 * @author elena
 */
public class SparkConfig {
    public static SparkSession getSession(){
        return SparkSession.builder()
                .appName("AI Impact Analysis")
                .config("spark.master", "local[*]")
                .getOrCreate();
        
    }
}
