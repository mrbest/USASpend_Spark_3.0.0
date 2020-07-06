package org.usaspend.execution

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{length, when}
import org.usaspend.root.SparkInitializer.spark


class ArchiveReader {

  def archive_reader(): DataFrame = {
    import spark.implicits._
    val usaspend1 = spark.read.
      option("header", "true").
      option("inferSchema", "true").
      option("sep", ",").csv("/Users/destiny/Documents/DockerStarts/Zeppelin_Docker_Share/sources/USASPENDFY19/FY2019_All_Contracts_Full_20200514_1.csv")

    val usaspend2 = spark.read.
      option("header", "true").
      option("inferSchema", "true").
      option("sep", ",").csv("/Users/destiny/Documents/DockerStarts/Zeppelin_Docker_Share/sources/USASPENDFY19/FY2019_All_Contracts_Full_20200514_2.csv")

    val usaspend3 = spark.read.
      option("header", "true").
      option("inferSchema", "true").
      option("sep", ",").csv("/Users/destiny/Documents/DockerStarts/Zeppelin_Docker_Share/sources/USASPENDFY19/FY2019_All_Contracts_Full_20200514_3.csv")

    val usaspend4 = spark.read.
      option("header", "true").
      option("inferSchema", "true").
      option("sep", ",").csv("/Users/destiny/Documents/DockerStarts/Zeppelin_Docker_Share/sources/USASPENDFY19/FY2019_All_Contracts_Full_20200514_4.csv")

    val usaspend5 = spark.read.
      option("header", "true").
      option("inferSchema", "true").
      option("sep", ",").csv("/Users/destiny/Documents/DockerStarts/Zeppelin_Docker_Share/sources/USASPENDFY19/FY2019_All_Contracts_Full_20200514_5.csv")

    val usaspend6 = spark.read.
      option("header", "true").
      option("inferSchema", "true").
      option("sep", ",").csv("/Users/destiny/Documents/DockerStarts/Zeppelin_Docker_Share/sources/USASPENDFY19/FY2019_All_Contracts_Full_20200514_6.csv")

    val usaspend7 = spark.read.
      option("header", "true").
      option("inferSchema", "true").
      option("sep", ",").csv("/Users/destiny/Documents/DockerStarts/Zeppelin_Docker_Share/sources/USASPENDFY19/FY2019_All_Contracts_Full_20200514_7.csv")

    val usaspend_a = usaspend1.unionByName(usaspend2)
    val usaspend_b = usaspend_a.unionByName(usaspend3)
    val usaspend_c = usaspend_b.unionByName(usaspend4)
    val usaspend_d = usaspend_c.unionByName(usaspend5)
    val usaspend_e = usaspend_d.unionByName(usaspend6)
    val usaspend_f = usaspend_e.unionByName(usaspend7)

    //filter out rows with bad psc codes
    //unable to resolve this parsing issue at this time
    val usaspend_g = usaspend_f.filter(length($"product_or_service_code") === 4).
    select($"award_or_idv_flag",
      $"parent_award_id_piid",
      $"award_id_piid",
      $"idv_type",
      $"product_or_service_code",
      $"naics_code",
      $"award_description",
      //$"v2_level_1_category",
      //$"v2_level_2_category",
      //$"v2_level_3_category",
      $"federal_action_obligation",
      $"recipient_name",
      $"contract_transaction_unique_key",
      $"action_date",
      $"funding_agency_name",
      $"awarding_agency_name")

    //create the unique_contract_id for all awards
    val usaspend_awards = usaspend_g.
      filter($"award_or_idv_flag" === "AWARD" ).
      withColumn("unique_contract_id", when(length($"parent_award_id_piid") < 3 || $"parent_award_id_piid"=== null, $"award_id_piid").otherwise($"parent_award_id_piid"))

    //create the unique_contract_id for all awards IDCs
    val usaspend_IDCs = usaspend_g.
      filter($"award_or_idv_flag" === "IDV" &&  $"idv_type" === "IDC" ).
      withColumn("unique_contract_id", when(length($"parent_award_id_piid") < 3 || $"parent_award_id_piid"=== null, $"award_id_piid").otherwise($"parent_award_id_piid"))

    //provide union of awards and IDCs
    val usaspend_award_and_IDC_spend = usaspend_awards.unionByName(usaspend_IDCs)

    //read the contract inventory
    val contract_inventory = spark.read.option("header", "true").option("inferSchema", "true").option("sep", "\t").csv("/Users/destiny/Documents/DockerStarts/Zeppelin_Docker_Share/sources/GSA_MASTER_CI2.txt")

    //join the contract inventory to the transactions
    val usaspend_award_and_IDC_spend_final = usaspend_award_and_IDC_spend.join(contract_inventory, Seq("unique_contract_id"), "left")

    //read taxonomy
    val taxonomy = spark.read.option("header", "true").option("inferSchema", "true").option("sep", "\t").csv("/Users/destiny/Documents/DockerStarts/Zeppelin_Docker_Share/sources/GWCM Category Mappings Table 01102020 - Sheet1.tsv")

    val gwcm_w_taxonomy1 = usaspend_award_and_IDC_spend_final.
        join(taxonomy, usaspend_award_and_IDC_spend_final("product_or_service_code") === taxonomy("psc_code") , "left_outer")

    gwcm_w_taxonomy1
  }




}
