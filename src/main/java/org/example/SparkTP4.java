package org.example;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import static org.apache.spark.sql.functions.col;

public class SparkTP4 {
    public static void main(String[] args) {
        // 1. Créer la session Spark
        SparkSession spark = SparkSession.builder()
                .appName("TP4 Spark SQL Java")
                .getOrCreate();

        // 2. Charger les données
        Dataset<Row> df = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv("src/main/resources/incidents.csv");

        System.out.println("Schéma du DataFrame:");
        df.printSchema();

        System.out.println("\nAperçu des données:");
        df.show(5);

        // Question 1: Nombre d'incidents par service
        System.out.println("\nNombre d'incidents par service:");
        df.groupBy("service")
                .count()
                .show();

        // Question 2: Les deux années avec le plus d'incidents
        System.out.println("\nDeux années avec le plus d'incidents:");
        df.withColumn("annee", functions.year(col("date")))
                .groupBy("annee")
                .count()
                .orderBy(col("count").desc())
                .limit(2)
                .show();

        // Fermer la session Spark
        spark.stop();
    }
}