package org.usaspend.transformers

import java.util.Calendar

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.usaspend.root.SparkInitializer._

class Utilities {

  import spark.implicits._


  def get_df_keys(source_df: DataFrame): List[Any] =
  {

    //capture category_mod transaction keys
    var transaction_keylist: List[Any] = List()

    println("collecting all the keys")
    var start_time = Calendar.getInstance()

    var keyquery = source_df.select($"contract_transaction_unique_key").collect()

    var end_time =  Calendar.getInstance()
    var duration = (end_time.getTimeInMillis() - start_time.getTimeInMillis() )

    //de-reference the keys in to the list
    for(inner <- keyquery)
    {
      if(inner != null)
      {
        val this_row = inner.getString(0)
        //println("Transaction key "+this_row + " found")
        transaction_keylist = transaction_keylist :+ this_row
      }//eo_if

    }//eo for

    println(duration/1000 +" seconds")

    transaction_keylist
  }


  def get_complement_df(transaction_keylist: List[Any], source_df:  DataFrame): DataFrame =
  {
    var start_time = Calendar.getInstance()

    var return_df = source_df.
      filter($"contract_transaction_unique_key".isin(transaction_keylist:_*) === false)
    var end_time =  Calendar.getInstance()
    var duration = (end_time.getTimeInMillis() - start_time.getTimeInMillis() )
    println(duration/1000 +" seconds")
    return_df
  }


  def exclusions_l2_adjuster(incoming: DataFrame): DataFrame = {
    //create df with transactions meeting the filter criteria
    //write the the new level 1 category identifier to column tempsub


    println("exclusions_l2_adjuster(): Querying l1 == \"Excluded\" for criteria")


    val category_mod1 = incoming.
      filter($"v2_level_1_category" === "Excluded").
      withColumn("tempsub", lit("") )


    //drop the level_2_category in the newly created df
    val category_mod2 = category_mod1.drop("v2_level_2_category")

    //rename tempsub column to level_1_category
    val category_mod3 = category_mod2.
      withColumnRenamed("tempsub", "v2_level_2_category")




    category_mod3.select($"contract_transaction_unique_key", $"v2_level_1_category", $"v2_level_2_category", $"v2_level_3_category", $"federal_action_obligation").
      withColumn("currency",  toCurrency(col("federal_action_obligation"))).show(false)

    category_mod3
  }



}
