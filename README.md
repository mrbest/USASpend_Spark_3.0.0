# USASpend_Spark_3.0.0
This application applies the GWCM contract inventory and category taxonomy to USA Spend archives.


The application is executed from SparkInitializer.scala within the following package: 
USASpend_Spark_3.0.0/src/main/scala/org/usaspend/root/

SparkInitializer.scala provides:
1. Spark engine init
2. Instantiates the supporting classes to deliver execution:
  a) USASpend_Spark_3.0.0/src/main/scala/org/usaspend/execution/ArchiveReader.scala
     Reads in the multi-part csv archive from USA Spend, unions them in to one archive, joins it with 
     the GWCM contract inventory and joins it with the GWCM Taxonomy file.
  b) USASpend_Spark_3.0.0/src/main/scala/org/usaspend/transformers/

