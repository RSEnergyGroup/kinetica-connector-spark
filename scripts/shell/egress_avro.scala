import com.databricks.spark.avro._

// table to read from
val TableName = "flights"

// config options
val KineticaOptions = Map(
    "database.jdbc_url" -> "jdbc:simba://gpudb:9292;ParentSet=test",
    "database.username" -> "",
    "database.password" -> "",
    "table.name" -> TableName,
    "spark.num_partitions" -> "4")

val tableDF = spark.read.format("com.kinetica.spark").options(KineticaOptions).load()

println(s"Schema for table: ${TableName}")
tableDF.printSchema()

println(s"Writing output lines. (rows = ${tableDF.count()})")
tableDF.coalesce(1).write.mode("overwrite").avro("output_avro")
