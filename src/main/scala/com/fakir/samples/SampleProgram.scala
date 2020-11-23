package com.fakir.samples

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SampleProgram {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)

    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    // Exercice 1

    println("> Exercice 1")

    val actor  = "Di Caprio"
    val films = sparkSession.sparkContext.textFile("data/films.csv")

    val search = films.filter(row => row.contains(actor))

    println("Nombre de film de "+actor+" : "+search.count())

    val searchTabbed = search.map(x => x.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
    val searchRatings = searchTabbed.map(x => x(2).toDouble)
    println("Moyenne des notes des films où jouent "+actor+" : "+searchRatings.mean)

    val filmsTabbed = films.map(x => x.split(";(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
    val filmsViewsSum = filmsTabbed.map(x => x(1).toDouble).sum
    val searchViewsSum = searchTabbed.map(x => x(1).toDouble).sum
    println("Pourcentage de vues par rapport à l'échantillon : "+searchViewsSum/filmsViewsSum * 100+" %" )


    println("Moyenne des notes par acteurs : ")
    val pairRatingsFilms = filmsTabbed.map(x => (x(3), x(2)))
    pairRatingsFilms.groupByKey().collect().foreach(x => {
      val iterator = x._2.iterator
      var sum = 0d
      var count = 0
      while (iterator.hasNext) {
        sum += iterator.next().toDouble
        count += 1
      }
      println("--> Notes moyennes de " + x._1 + " : " + sum / count)
    })

    println("Moyenne des notes par acteurs : ")
    val pairViewsFilms = filmsTabbed.map(x => (x(3), x(1)))
    pairViewsFilms.groupByKey().collect().foreach(x => {
      val iterator = x._2.iterator
      var sum = 0d
      var count = 0
      while (iterator.hasNext) {
        sum += iterator.next().toDouble
        count += 1
      }
      println("--> Vues moyennes de " + x._1 + " : " + sum / count)
    })


    // Exercice 2

    println("> Exercice 2")

    val df = sparkSession.read
      .option("header", false).option("delimiter", ";").option("inferSchema", true)
      .csv("data/films.csv")
      .toDF("nom_film", "nombre_vues", "note_film", "acteur_principal")

    val dfSearch = df.filter(df.col("acteur_principal").contains(actor))
    println("Nombre de film de "+actor+" : "+dfSearch.count())

    val dfSearchRatingsMean = dfSearch.agg(mean(col("note_film"))).first.getDouble(0)
    println("Moyenne des notes des films où jouent "+actor+" : "+dfSearchRatingsMean)

    val dfFilmsViewsSum = df.agg(sum(col("nombre_vues"))).first.getLong(0).toDouble
    val dfSearchViewsSum = dfSearch.agg(sum(col("nombre_vues"))).first.getLong(0).toDouble
    println("Pourcentage de vues par rapport à l'échantillon : "+(dfSearchViewsSum/dfFilmsViewsSum)*100+" %" )

    val dfMeanGroupByActors = df.groupBy("acteur_principal").mean("note_film").as("moyenne_note_film")
    println("Moyennes des notes par acteur :")
    dfMeanGroupByActors.show(false)

    val dfAvgViewsGroupByActors = df.groupBy("acteur_principal")
      .agg(((sum("nombre_vues")/dfFilmsViewsSum) *100).alias("pourcentage_de_vues"))

    val dfFinal = df.join(dfAvgViewsGroupByActors, Seq("acteur_principal"),"right_outer")
    dfFinal.show(false)
  }
}