package sql_practice

import org.apache.spark.sql.functions._
import spark_helpers.SessionBuilder

object examples {

  def exec1(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val toursDF = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")
    toursDF.show

    println(toursDF
      .select(explode($"tourTags"))
      .groupBy("col")
      .count()
      .count()
    )

    toursDF
      .select(explode($"tourTags"), $"tourDifficulty")
      .groupBy($"col", $"tourDifficulty")
      .count()
      .orderBy($"count".desc)
      .show(10)

    toursDF.select($"tourPrice")
      .filter($"tourPrice" > 500)
      .orderBy($"tourPrice".desc)
      .show(20)


  }

  def exec2(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val demo = spark.read
      .option("mode", "PERMISSIVE")
      .json("data/input/demographie_par_commune.json")

    val nbrInhabitants = demo.select($"Population").agg(sum($"Population"))
    nbrInhabitants.show()

    val topPopDep = demo.groupBy("Departement").agg(sum($"population") as "population").orderBy($"population".desc)
    topPopDep.show()

    val dep = spark.read
      .option("mode", "PERMISSIVE")
      .csv("data/input/departements.txt")

    val dfdep = dep.as("dfdep")
    val dfpop = topPopDep.as("dfdemo")

    dfpop.join(dfdep, col("dfdemo.departement")===col("dfdep._c1")).select($"dfdep._c0" as "departement", $"dfdep._c1" as "code", $"dfdemo.population" as "population").show()

  }

  def exec3(): Unit ={
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val s_07_raw = spark.read
      .option("mode", "PERMISSIVE")
      .option("sep", "\t")
      .csv("data/input/sample_07")

    val s_07 = s_07_raw.select($"_c0" as "code", $"_c1" as "description", $"_c2" as "total_emp", $"_c3" as "salary").as("s_07")

    val s_08_raw = spark.read
      .option("mode", "PERMISSIVE")
      .option("sep", "\t")
      .csv("data/input/sample_08")

    val s_08 = s_08_raw.select($"_c0" as "code", $"_c1" as "description", $"_c2" as "total_emp", $"_c3" as "salary").as("s_08")

    val topSalaries7 = s_07
      .select("*")
      .where($"salary" >= 100000)
      .orderBy($"salary".desc)
    topSalaries7.show()

    val salaryGrowth = s_07
      .join(s_08, col("s_07.code") === col ("s_08.code"))
      .select($"s_07.code", $"s_07.description", $"s_08.salary" - $"s_07.salary" as "salary_growth")
      .where($"salary_growth" > 0)
      .orderBy($"salary_growth".desc)
      .as("sg")
    salaryGrowth.show()

    val jobLoss = s_07
      .join(s_08, col("s_07.code") === col ("s_08.code"))
      .select($"s_07.code", $"s_07.description", $"s_07.total_emp" - $"s_08.total_emp" as "job_loss", $"s_07.salary")
      .where($"job_loss" > 0)
      .where($"s_07.salary" > 100000)
      .orderBy($"job_loss".desc)
    jobLoss.show()

  }

  def exec4(): Unit = {
    val spark = SessionBuilder.buildSession()
    import spark.implicits._

    val tours = spark.read
      .option("multiline", true)
      .option("mode", "PERMISSIVE")
      .json("data/input/tours.json")

    tours.show

    val nbrLevels = tours.select($"tourDifficulty").distinct().count()
    System.out.println("Number of levels of difficulty: " + nbrLevels)

    val minMaxAvg = tours.select(min($"tourPrice"), max($"tourPrice"), avg($"tourPrice"))
    minMaxAvg.show()

    val minMaxAvgByLevel = tours
      .groupBy($"tourDifficulty")
      .agg(min($"tourPrice"), max($"tourPrice"), avg($"tourPrice"))
    minMaxAvgByLevel.show()

    val minMaxAvgByLevel2 = tours
      .groupBy($"tourDifficulty")
      .agg(min($"tourPrice"), max($"tourPrice"), avg($"tourPrice"), min($"tourLength"), max($"tourLength"), avg($"tourLength"))
    minMaxAvgByLevel2.show()

    val topTags = tours
      .select(explode($"tourTags") as "tags")
      .groupBy("tags")
      .count()
      .orderBy($"count".desc)
      .limit(10)
    topTags.show()

    val relationTagDifficulty = tours
      .select($"tourDifficulty", explode($"tourTags") as "tags")
      .groupBy($"tourDifficulty", $"tags")
      .count()
      .orderBy($"count".desc, $"tourDifficulty".desc)
    relationTagDifficulty.show()

    val relationTagDifficultyPrice = tours
      .select($"tourDifficulty", explode($"tourTags") as "tags", $"tourPrice")
      .groupBy($"tourDifficulty", $"tags")
      .agg(min($"tourPrice"), max($"tourPrice"), avg($"tourPrice"))
      .orderBy($"avg(tourPrice)".desc, $"tourDifficulty".desc)
    relationTagDifficultyPrice.show()



  }

}
