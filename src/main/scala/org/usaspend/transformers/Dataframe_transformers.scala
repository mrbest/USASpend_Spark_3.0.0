package org.usaspend.transformers

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
//import org.apache.spark.sql._
import org.usaspend.root.SparkInitializer._
class Dataframe_transformers {
  import spark.implicits._

  def psc_naics_combinations(psc: String, naicsList: List[String] = List(), new_level_2: String, gwcm_w_taxonomy1: DataFrame): DataFrame = {

    //create df with transactions meeting the filter criteria
    //write the the new level 2 category identifier to column tempsub

    println("psc_naics_combinations(): Querying gwcm_in for criteria")

    val  category_mod1 = gwcm_w_taxonomy1.
      filter($"product_or_service_code" === psc).// && $"naics_code" === "923130").
      filter($"naics_code".isin(naicsList:_*) === true).
      withColumn("v2_level_2_category", lit(new_level_2) )

    category_mod1.select($"contract_transaction_unique_key",$"v2_level_2_category", $"federal_action_obligation").
      withColumn("currency",  toCurrency(col("federal_action_obligation"))).show(false)
    category_mod1
  }


  def category_refinement_keyword_inclusion(pscList: List[String], targetRegex: String, new_level_2: String, new_level_3: String, gwcm_w_taxonomy1: DataFrame): DataFrame = {
    //create df with transactions meeting the filter criteria
    //write the the new level 2 category identifier to column tempsub
    var category_mod3: DataFrame = null
    if(new_level_3 == null) //resetting level 2 category only
    {
      val  category_mod1 = gwcm_w_taxonomy1.
        filter($"product_or_service_code".isin(pscList:_*) === true).
        filter($"award_description".rlike(targetRegex) ).
        withColumn("v2_level_2_category", lit(new_level_2) )

      category_mod3 = category_mod1

    }
    else //resetting level 2 and 3 category only
    {
      println("category_refinement_keyword_inclusion(): Querying gwcm_in for criteria")

      val  category_mod1 = gwcm_w_taxonomy1.
        filter($"product_or_service_code".isin(pscList:_*) === true).
        filter($"award_description".rlike(targetRegex) ).
        withColumn("v2_level_2_category", lit(new_level_2) ).
        withColumn("v2_level_3_category", lit(new_level_3) )

      category_mod3 = category_mod1

    }
    category_mod3.select($"contract_transaction_unique_key", $"v2_level_1_category", $"v2_level_2_category", $"v2_level_3_category", $"federal_action_obligation").
      withColumn("currency",  toCurrency(col("federal_action_obligation"))).show(false)

    category_mod3
  }



  def exclusions(pscList: List[String], targetRegex: String, new_level_1:String, gwcm_w_taxonomy1: DataFrame): DataFrame = {
    //create df with transactions meeting the filter criteria
    //write the the new level 1 category identifier to column tempsub


    println("exclusions(): Querying gwcm_in for criteria")

    val category_mod1 = gwcm_w_taxonomy1.
      filter($"product_or_service_code".isin(pscList:_*) === true).
      filter($"award_description".rlike(targetRegex) ).
      withColumn("v2_level_1_category", lit(new_level_1) )

    val category_mod3 = category_mod1
    //  withColumnRenamed("tempsub", "v2_level_1_category")

    category_mod3.select($"contract_transaction_unique_key", $"v2_level_1_category", $"v2_level_2_category", $"v2_level_3_category", $"federal_action_obligation").
      withColumn("currency",  toCurrency(col("federal_action_obligation"))).show(false)

    category_mod3
  }


  def prison_exclusion(pscList: List[String], targetRegex: String, new_level_1:String, new_level_3: String, gwcm_w_taxonomy1:DataFrame): DataFrame = {

    //resetting level 1 and 3 category only

    println("prison_exclusions(): resetting level 1 and 3 category")
    //start_time = Calendar.getInstance()

    val  category_mod1 = gwcm_w_taxonomy1.
      filter($"product_or_service_code".isin(pscList:_*) === true).
      filter($"award_description".rlike(targetRegex) ).
      withColumn("v2_level_1_category", lit(new_level_1) ).
      withColumn("v2_level_3_category", lit(new_level_3) )

    category_mod1.select($"contract_transaction_unique_key", $"v2_level_1_category", $"v2_level_2_category", $"v2_level_3_category", $"federal_action_obligation").
      withColumn("currency",  toCurrency(col("federal_action_obligation"))).show(false)


    category_mod1

  }

  def dod_exclusions(pscList: List[String], gwcm_w_taxonomy1:DataFrame): DataFrame = {
    //create df with transactions meeting the filter criteria
    //write the the new level 1 category identifier to column tempsub
    //var category_mod3: DataFrame = null

    val new_level_1 = "Excluded"
    val category_mod1 = gwcm_w_taxonomy1.
      filter($"product_or_service_code".isin(pscList:_*) === true).
      filter($"awarding_agency_name" === "DEPARTMENT OF DEFENSE (DOD)" || $"funding_agency_name" === "DEPARTMENT OF DEFENSE (DOD)").
      withColumn("v2_level_1_category", lit(new_level_1) )

    category_mod1.select($"contract_transaction_unique_key", $"v2_level_1_category", $"v2_level_2_category", $"v2_level_3_category", $"federal_action_obligation").
      withColumn("currency",  toCurrency(col("federal_action_obligation"))).show(false)

    category_mod1
  }


  //Function definition for Vendor Exclusions

  def vendor_exclusions(pscList: List[String], targetRegex: String, gwcm_w_taxonomy1:DataFrame): DataFrame = {
    //create df with transactions meeting the filter criteria
    //write the the new level 1 category identifier to column tempsub
    //var category_mod3: DataFrame = null

    val new_level_1 = "Excluded"
    val category_mod1 = gwcm_w_taxonomy1.
      filter($"product_or_service_code".isin(pscList:_*) === true).
      filter($"recipient_name".rlike(targetRegex) ).
      withColumn("v2_level_1_category", lit(new_level_1) )

    category_mod1.select($"contract_transaction_unique_key", $"v2_level_1_category", $"v2_level_2_category", $"v2_level_3_category", $"federal_action_obligation").
      withColumn("currency",  toCurrency(col("federal_action_obligation"))).show(false)

    category_mod1
  }

  def exclusions_l2_adjuster(incoming: DataFrame): DataFrame = {
    //create df with transactions meeting the filter criteria
    //write the the new level 1 category identifier to column tempsub


    println("exclusions_l2_adjuster(): Querying l1 == \"Excluded\" for criteria")

    val category_mod1 = incoming.
      filter($"v2_level_1_category" === "Excluded").
      withColumn("v2_level_2_category", lit("") )

    category_mod1.select($"contract_transaction_unique_key", $"v2_level_1_category", $"v2_level_2_category", $"v2_level_3_category", $"federal_action_obligation").
      withColumn("currency",  toCurrency(col("federal_action_obligation"))).show(false)

    category_mod1
  }


}
