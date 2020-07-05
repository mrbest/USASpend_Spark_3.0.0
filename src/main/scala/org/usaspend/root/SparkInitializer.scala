package org.usaspend.root

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.usaspend.execution.ArchiveReader
import org.usaspend.transformers.Dataframe_transformers
import org.usaspend.transformers.Utilities


object SparkInitializer  {
  //import scala.util.Failure

  import org.apache.spark.sql.functions._

  val spark = SparkSession.builder.
    master("local[*]").
    //config("spark.driver.host", "127.0.0.1").
    config("spark.executor.memory", "48g").
    config("spark.driver.memory", "48g").
    config("spark.memory.offHeap.enabled",true).
    config("spark.memory.offHeap.size","32g").
    appName("USA_Spend_GWCM").
    getOrCreate()

  import spark.implicits._
  spark.sparkContext.setLogLevel("ERROR")

  val formatter = java.text.NumberFormat.getCurrencyInstance
  //UDF Definition
  def toCurrencyFun(dbl: Double):Option[String] = {
    val s = Option(dbl).getOrElse(return None)
    Some(formatter.format(s))
  }


  val toCurrency = udf[Option[String], Double](toCurrencyFun)


  def main(args: Array[String]) {
    //initialize the archive reader and read in the files
    val archive_reader = new ArchiveReader()
    val archive = archive_reader.archive_reader()//.cache()
    archive.select($"v2_level_1_category").distinct().show(false)

/*
    val transformer = new Dataframe_transformers()
    // 3.2.1 Compensation and Benefits
    // Summary: Update level_2_category to “Compensation and Benefits” for all rows that have psc code “R431” and naics code “923130”
    var list1 = List("923130")
    val post_compensation_benfits_df = transformer.psc_naics_combinations("R431", list1, "Compensation and Benefits", archive)


    //3.2.2 Employee Relations
    //Summary: Update level_2_category to “Employee Relations” for all rows that have psc code “R431” and naics code “561611”
    list1 = List("561611")
    val post_employee_relations_df = transformer.psc_naics_combinations("R431", list1, "Employee Relations", archive)

    //3.2.3 Human Capital Evaluation
    //Summary: Update level_2_category to “Human Capital Evaluation” for all rows that have psc code “R431” and naics code “541611” or “813930”
    list1 = List("541611", "813930")
    val post_human_capital_eval_df = transformer.psc_naics_combinations("R431", list1, "Human Capital Evaluation", archive)

    //3.2.4 Strategy, Policies, and Operations Plan
    //Summary: Update level_2_category to “Strategy, Policies, and Operations Plan” for all rows that have psc code “R431” and naics code “541612”
    list1 = List("541612")
    val strategy_policies_and_operations = transformer.psc_naics_combinations("R431", list1, "Strategy, Policies, and Operations Plan", archive)

    //3.2.5 Talent Acquisition
    //Summary: Update level_2_category to “Talent Acquisition” for all rows that have psc code “R431” and naics code “561311” or “561312” or “561320” or “561330”
    list1 = List("561311", "561312", "561320", "561330")
    val post_talent_acquisition_df = transformer.psc_naics_combinations("R431", list1, "Talent Acquisition", archive)

    //3.3.1 Security Systems/Tactical Communication Equipment
    //Summary: Update level_2_category to “Security Systems” and level 3 category to “Tactical Communication Equipment” for all rows that have psc code (6350, 6665, 8465, 8470, J063, N063, 206, W063) and description of requirement contains “RADIO”
    list1 = List("6350", "6665", "8465", "8470", "J063", "N063", "S206", "W063")
    var  secRegex1 = ".*(?i)(RADIO).*"
    val secsys_taccom_equipment = transformer.category_refinement_keyword_inclusion(list1, secRegex1, "Security Systems", "Tactical Communication Equipment", archive)

    //3.3.2 Protective Apparel and Equipment/Armored Vehicle Purchase
    //Summary: Update level_2_category to “Protective Apparel and Equipment” and level 3 category to “Armored Vehicle Purchase” for all rows that have psc code (V127) and description of requirement contains “PURCHASE”
    list1 = List("V127")
    secRegex1 = ".*(?i)(PURCHASE).*"
    val  protective_apparel_equipment_vehicle_purchase = transformer.category_refinement_keyword_inclusion(list1, secRegex1, "Protective Apparel and Equipment", "Armored Vehicle Purchase", archive)

    //3.3.3 Protective Apparel and Equipment/Body Armor
    //Summary: Update level_2_category to “Protective Apparel and Equipment” and level 3 category to “Body Armor” for all rows that have psc code (8465, 8470) and description of requirement contains “ARMOR” or “PLATE”
    list1 = List("8465", "8470")
    secRegex1 = ".*(?i)(ARMOR|PLATE).*"
    val   protective_apparel_equipment_body_armor = transformer.category_refinement_keyword_inclusion(list1, secRegex1, "Protective Apparel and Equipment", "Body Armor", archive)

    //3.3.4 Protective Apparel and Equipment/Law Enforcement Equipment and Apparel
    //Summary: Update level_2_category to “Protective Apparel and Equipment” and level 3 category to “Law Enforcement Equipment and Apparel” for all rows that have psc code (8465, 8470)
    // and description of requirement contains "EAR PLUGS" or "TACTICAL GEAR" or "PISTOL HOLDER" or "HOLSTER" or "VISION" or "BREATHALYZER" or "KNIFE" or "KNIVES" or "GOGGLE" or "LASER" or "SIGHT" or "BINOCULAR" or "BATON" or "RESTRAINT" or "HANDCUFF"
    list1 = List("8465", "8470")
    var  secRegex = ".*(?i)(EAR PLUGS|TACTICAL GEAR|PISTOL HOLDER|HOLSTER|VISION|BREATHALYZER|KNIFE|KNIVES|GOGGLE|LASER|SIGHT|BINOCULAR|BATON|RESTRAINT|HANDCUFF).*"
    val   protective_apparel_equipment_law_enforcement = transformer.category_refinement_keyword_inclusion(list1, secRegex1, "Protective Apparel and Equipment", "Law Enforcement Equipment and Apparel", archive )

    //3.4.1 Excluded/Prison Detention Supplies
    //Summary: Update level 1 category to “Excluded” and level 3 category to “Prison and Detention Supplies” for all rows that have psc code (1005, 1010, 1305, 1310, 1330, 1370, 3770,           	5660, 6350, 6665, 6710, 6720, 8465, 8470, 8710, 8820, J063, J088, K063, K088, L063, N063, N088, R430, S206, S211, V127, W063, W088) and description of requirement contains "INMATE" or "DETENTION" or "PRISON" or "FOOD SERVICE" or "UNIFORM"
    list1 = List("1005", "1010", "1305", "1310", "1330", "1370", "3770", "5660", "6350", "6665", "6710", "6720", "8465", "8470", "8710", "8820", "J063", "J088","K063", "K088", "L063", "N063", "N088", "R430", "S206", "S211", "V127", "W063", "W088")
    secRegex1 = ".*(?i)(INMATE|DETENTION|PRISON|FOOD SERVICE|UNIFORM).*"
    val excluded_Prison_Detention_Supplies = transformer.prison_exclusion(list1, secRegex1, "Excluded", "Prison and Detention Supplies", archive)

    //3.4.2 Excluded/Department of Defense
    //Summary: Update level 1 category to “Excluded” for all rows that have psc code (1005, 1010, 1305, 1310, 1330, 1370, 8465, 8470, V127) and contracting or funding department is “DOD”
    list1 = List("1005", "1010", "1305", "1310", "1330", "1370", "8465", "8470", "V127")
    val excluded_Department_of_Defense = transformer.dod_exclusions(list1, archive)

    //3.4.3 Excluded/Hotels and Motels
    //Summary: Update level 1 category to “Excluded” for all rows that have psc code (V127) and description of requirement contains “HOTEL” or “MOTEL”
    list1 = List("V127")
    secRegex1 = ".*(?i)(HOTEL|MOTEL).*"
    val excluded_Hotels_and_Motels =  transformer.exclusions(list1, secRegex1, "Excluded", archive)

    //3.4.4 Excluded/Farm and Agricultural  (Vendor)
    //Summary: Update level 1 category to “Excluded” for all rows that have psc code (3770, 8710, 8820, J088, K088, N088, W088) and vendor name contains "LAB" or "FARM" or "AGRI" or "SEED" or "F EST" or "GARDEN" or "NURSERY" or "BIO" or "NURSERIES" or "AQUA" or "GENETICS" or "FOLIAGE" or "SWINE" or "RESEARCH" or "LANDSCAPE" or "H TICULT" or "CATTLE" or  "BRUSH" or "RANCH" or "AVI" or "API" or "VEG" or "ARB " or "C N" or "WHEAT" or "SCIEN" or "FRUIT" or "PRODUCE" or "FOOD" or "FL " or "PLANT" or "SOD" or "GROW" or "PALM" or "LANDSCAPE" or "TIMBER" or "HEALTH" or "ACRES" or "BEES" or "P K" or "PRIMATE" or "SOIL" or "EXCAV" or "CATER" or "AGR" or "ECO" or "DAIRY" or "ENVI " or "RAPT " or "MEAT" or "GREEN" or "MULCH" or "FISH" or "REEF" or "LUMBER" or "HOME" or "LAND" or "TREE" or "WOOD" or "SHRIMP" or "MILL" or "OIL" or "TOOL" or "CHIMP" or "RECLAM" or "BISON" or "COAL" or "GRASS" or "SOY" or "GROUND" or "RODENT" or "CHIPS" or "MOLASSES" or "DIABE" or "CALF" or "POULTRY" or "VEG" or "BURRO" or "SEA LION" or "FERAL" or "SWIM" or "SWINE" or "HOG" or "JP MINE" or "JAGER PRO" or "CAISSON" or "BABOON" or "BASIL" or·"SILAGE" or "CATTLE" or "BULL" or "TRAPS"
    list1 = List("3770", "8710", "8820", "J088", "K088", "N088", "W088")
    secRegex1 = ".*(?i)(LAB|FARM|AGRI|SEED|F EST|GARDEN|NURSERY|BIO|NURSERIES|AQUA|GENETICS|FOLIAGE|SWINE|RESEARCH|LANDSCAPE|H TICULT|CATTLE|BRUSH|RANCH|AVI|API|VEG|ARB|C N|WHEAT|SCIEN|FRUIT|PRODUCE|FOOD|FL |PLANT|SOD|GROW|PALM|LANDSCAPE|TIMBER|HEALTH|ACRES|BEES|P K|PRIMATE|SOIL|EXCAV|CATER|AGR|ECO|DAIRY|ENVI |RAPT |MEAT|GREEN|MULCH|FISH|REEF|LUMBER|HOME|LAND|TREE|WOOD|SHRIMP|MILL|OIL|TOOL|CHIMP|RECLAM|BISON|COAL|GRASS|SOY|GROUND|RODENT|CHIPS|MOLASSES|DIABE|CALF|POULTRY|VEG|BURRO|SEA LION|FERAL|SWIM|SWINE|HOG|JP MINE|JAGER PRO|CAISSON|BABOON|BASIL|SILAGE|CATTLE|BULL|TRAPS).*"
    val excluded_farm_and_Agricultural_Vendor = transformer.vendor_exclusions(list1, secRegex1, archive)

    //3.4.5 Excluded/Farm and Agricultural (Awards)
    //Summary: Update level 1 category to “Excluded” for all rows that have psc code (3770, 8710, 8820, J088, K088, N088, W088) and description or requirement contains "LAB" or "FARM" or "AGRI" or "SEED" or "F EST" or "GARDEN" or "NURSERY" or "BIO" or "NURSERIES" or "AQUA" or "GENETICS" or "FOLIAGE" or "SWINE" or "RESEARCH" or "LANDSCAPE" or "H TICULT" or "CATTLE" or  "BRUSH" or "RANCH" or "AVI" or "API" or "VEG" or "ARB " or "C N" or "WHEAT" or "SCIEN" or "FRUIT" or "PRODUCE" or "FOOD" or "FL " or "PLANT" or "SOD" or "GROW" or "PALM" or "LANDSCAPE" or "TIMBER" or "HEALTH" or "ACRES" or "BEES" or "P K" or "PRIMATE" or "SOIL" or "EXCAV" or "CATER" or "AGR" or "ECO" or "DAIRY" or "ENVI " or "RAPT " or "MEAT" or "GREEN" or "MULCH" or "FISH" or "REEF" or "LUMBER" or "HOME" or "LAND" or "TREE" or "WOOD" or "SHRIMP" or "MILL" or "OIL" or "TOOL" or "CHIMP" or "RECLAM" or "BISON" or "COAL" or "GRASS" or "SOY" or "GROUND" or "RODENT" or "CHIPS" or "MOLASSES" or "DIABE" or "CALF" or "POULTRY" or "VEG" or "BURRO" or "SEA LION" or "FERAL" or "SWIM" or "SWINE" or "HOG" or "JP MINE" or "JAGER PRO" or "CAISSON" or "BABOON" or "BASIL" or “SILAGE" or "CATTLE" or "BULL" or "TRAPS"
    list1 = List("3770", "8710", "8820", "J088", "K088", "N088", "W088")
    secRegex1 = ".*(?i)(LAB|FARM|AGRI|SEED|F EST|GARDEN|NURSERY|BIO|NURSERIES|AQUA|GENETICS|FOLIAGE|SWINE|RESEARCH|LANDSCAPE|H TICULT|CATTLE|BRUSH|RANCH|AVI|API|VEG|ARB|C N|WHEAT|SCIEN|FRUIT|PRODUCE|FOOD|FL |PLANT|SOD|GROW|PALM|LANDSCAPE|TIMBER|HEALTH|ACRES|BEES|P K|PRIMATE|SOIL|EXCAV|CATER|AGR|ECO|DAIRY|ENVI |RAPT |MEAT|GREEN|MULCH|FISH|REEF|LUMBER|HOME|LAND|TREE|WOOD|SHRIMP|MILL|OIL|TOOL|CHIMP|RECLAM|BISON|COAL|GRASS|SOY|GROUND|RODENT|CHIPS|MOLASSES|DIABE|CALF|POULTRY|VEG|BURRO|SEA LION|FERAL|SWIM|SWINE|HOG|JP MINE|JAGER PRO|CAISSON|BABOON|BASIL|SILAGE|CATTLE|BULL|TRAPS).*"
    val excluded_Farm_and_Agricultural_awards =  transformer.exclusions(list1, secRegex1, "Excluded", archive)

    //3.4.6 Excluded/Other Non-S&P
    // Summary: Update level 1 category to “Excluded” for all rows that have psc code (8465, 8470) and description or requirement contains "SLEEPING BAG" or "TREADMILL" or "NON-MONETARY AWARDS" or "ELLIPTICAL" or "ELLIPTICAL" or "CROSS TRAINER" or "FLIGHT HELMETS" or "FEL" or "LINEN" or "DOCK LEVELER" or "SWITCH" or "FIRE" or "WEIGHT EQUIPMENT" or "POWER TOOLS" or "GENERATOR" or "TABLET" or "ALARM" or "PLOTTER" or "SHREDDER" or "CAR RENTAL" or "SNOW REMOVAL" or "MUSIC PROGRAM" or "LAWN MOWER" or "CAMELBACK" or "BACKPACK" or "NON-TRADITIONAL AWARD" or "SUNSCREEN" or "LOTION" or "COOLER" or "REPELLANT" or "SHOWER" or "PENS" or "PENCILS" or "PROMOTIONAL" or "NECKLACE" or "PERSONAL BAGS"
    list1 = List("8465", "8470")
    secRegex1 = ".*(?i)(SLEEPING BAG|TREADMILL|NON-MONETARY AWARDS|ELLIPTICAL|ELLIPTICAL|CROSS TRAINER|FLIGHT HELMETS|FEL|LINEN|DOCK LEVELER|SWITCH|FIRE|WEIGHT EQUIPMENT|POWER TOOLS|GENERATOR|TABLET|ALARM|PLOTTER|SHREDDER|CAR RENTAL|SNOW REMOVAL|MUSIC PROGRAM|LAWN MOWER|CAMELBACK|BACKPACK|NON-TRADITIONAL AWARD|SUNSCREEN|LOTION|COOLER|REPELLANT|SHOWER|PENS|PENCILS|PROMOTIONAL|NECKLACE|PERSONAL BAGS).*"
    val excluded_Other_Non_S_P =  transformer.exclusions(list1, secRegex1, "Excluded", archive)

    val util = new Utilities()

    val post_compensation_benfits_list = util.get_df_keys(post_compensation_benfits_df)
    val post_compensation_benefits_df_complement = util.get_complement_df(post_compensation_benfits_list, archive)
    val post_compensation_benefits_final = post_compensation_benfits_df.unionByName(post_compensation_benefits_df_complement)

    val post_employee_relations_list = util.get_df_keys(post_employee_relations_df)
    val post_employee_relations_df_complement = util.get_complement_df(post_employee_relations_list, post_compensation_benefits_final)
    val post_employee_relations_final = post_employee_relations_df.unionByName(post_employee_relations_df_complement)

    val post_human_capital_eval_list = util.get_df_keys(post_human_capital_eval_df)
    val post_human_capital_eval_df_complement = util.get_complement_df(post_human_capital_eval_list, post_employee_relations_final)
    val post_human_capital_eval_final = post_human_capital_eval_df.unionByName(post_human_capital_eval_df_complement)

    val strategy_policies_and_operations_list = util.get_df_keys(strategy_policies_and_operations)
    val strategy_policies_and_operations_df_complement = util.get_complement_df(strategy_policies_and_operations_list, post_human_capital_eval_final)
    val strategy_policies_and_operations_final = strategy_policies_and_operations.unionByName(strategy_policies_and_operations_df_complement)

    val post_talent_acquisition_list = util.get_df_keys(post_talent_acquisition_df)
    val post_talent_acquisition_df_complement = util.get_complement_df(post_talent_acquisition_list, strategy_policies_and_operations_final)
    val post_talent_acquisition_final = post_talent_acquisition_df.unionByName(post_talent_acquisition_df_complement)

    val secsys_taccom_equipment_list = util.get_df_keys(secsys_taccom_equipment)
    val secsys_taccom_equipment_df_complement = util.get_complement_df(secsys_taccom_equipment_list, post_talent_acquisition_final)
    val secsys_taccom_equipment_final = secsys_taccom_equipment_df_complement.unionByName(secsys_taccom_equipment)

    val protective_apparel_equipment_vehicle_purchase_list = util.get_df_keys(protective_apparel_equipment_vehicle_purchase)
    val protective_apparel_equipment_vehicle_purchase_complement = util.get_complement_df(protective_apparel_equipment_vehicle_purchase_list, secsys_taccom_equipment_final)
    val protective_apparel_equipment_vehicle_purchase_final = protective_apparel_equipment_vehicle_purchase_complement.unionByName(protective_apparel_equipment_vehicle_purchase)

    val protective_apparel_equipment_body_armor_list = util.get_df_keys(protective_apparel_equipment_body_armor)
    val protective_apparel_equipment_body_armor_df_complement = util.get_complement_df(protective_apparel_equipment_body_armor_list, protective_apparel_equipment_vehicle_purchase_final)
    val protective_apparel_equipment_body_armor_final = protective_apparel_equipment_body_armor_df_complement.unionByName(protective_apparel_equipment_body_armor)

    val protective_apparel_equipment_law_enforcement_list = util.get_df_keys(protective_apparel_equipment_law_enforcement)
    val protective_apparel_equipment_law_enforcement_df_complement = util.get_complement_df(protective_apparel_equipment_law_enforcement_list, protective_apparel_equipment_body_armor_final)
    val protective_apparel_equipment_law_enforcement_final = protective_apparel_equipment_law_enforcement_df_complement.unionByName(protective_apparel_equipment_law_enforcement)

    val excluded_Prison_Detention_Supplies_list = util.get_df_keys(excluded_Prison_Detention_Supplies)
    val excluded_Prison_Detention_Supplies_complement = util.get_complement_df(excluded_Prison_Detention_Supplies_list, /*protective_apparel_equipment_law_enforcement_final*/ protective_apparel_equipment_law_enforcement_final)
    val excluded_Prison_Detention_Supplies_final = excluded_Prison_Detention_Supplies.unionByName(excluded_Prison_Detention_Supplies_complement)

    val excluded_Department_of_Defense_list = util.get_df_keys(excluded_Department_of_Defense)
    val excluded_Department_of_Defense_complement = util.get_complement_df(excluded_Department_of_Defense_list, excluded_Prison_Detention_Supplies_final)
    val excluded_Department_of_Defense_final = excluded_Department_of_Defense_complement.unionByName(excluded_Department_of_Defense)

    val excluded_Hotels_and_Motels_list = util.get_df_keys(excluded_Hotels_and_Motels)
    val excluded_Hotels_and_Motels_complement = util.get_complement_df(excluded_Hotels_and_Motels_list, excluded_Department_of_Defense_final)
    val excluded_Hotels_and_Motels_final = excluded_Hotels_and_Motels_complement.unionByName(excluded_Hotels_and_Motels)

    val excluded_farm_and_Agricultural_Vendor_list = util.get_df_keys(excluded_farm_and_Agricultural_Vendor)
    val excluded_farm_and_Agricultural_Vendor_complement = util.get_complement_df(excluded_farm_and_Agricultural_Vendor_list, excluded_Hotels_and_Motels_final)
    val excluded_farm_and_Agricultural_Vendor_final = excluded_farm_and_Agricultural_Vendor_complement.unionByName(excluded_farm_and_Agricultural_Vendor)

    val excluded_Farm_and_Agricultural_awards_list = util.get_df_keys(excluded_Farm_and_Agricultural_awards)
    val excluded_Farm_and_Agricultural_awards_complement = util.get_complement_df(excluded_Farm_and_Agricultural_awards_list, excluded_farm_and_Agricultural_Vendor_final)
    val excluded_Farm_and_Agricultural_awards_final = excluded_Farm_and_Agricultural_awards_complement.unionByName(excluded_Farm_and_Agricultural_awards)

    val excluded_Other_Non_S_P_list = util.get_df_keys(excluded_Other_Non_S_P)
    val excluded_Other_Non_S_P_complement = util.get_complement_df(excluded_Other_Non_S_P_list, excluded_Farm_and_Agricultural_awards_final)
    val excluded_Other_Non_S_P_final = excluded_Other_Non_S_P_complement.unionByName(excluded_Other_Non_S_P).cache()

    val l1_exclusion_level_blanker_df = util.exclusions_l2_adjuster(excluded_Other_Non_S_P_final)
    //excluded_Other_Non_S_P
    val exclusions_marker_list = util.get_df_keys(l1_exclusion_level_blanker_df)
    val exclusions_marker_complement = util.get_complement_df(exclusions_marker_list, excluded_Other_Non_S_P_final)
    val final_df = l1_exclusion_level_blanker_df.unionByName(exclusions_marker_complement).cache()
*/
    //excluded_Other_Non_S_P_final.write.save("/Users/destiny/Documents/DockerStarts/Zeppelin_Docker_Share/sources/USASPENDFY19/USASpendGWCMFY19_from_app.parquet_mini")
    archive.coalesce(1).write.option("header", "true").option("sep", ",").csv("/Users/destiny/Documents/DockerStarts/Zeppelin_Docker_Share/sources/USASPENDFY19/usaspend_award_and_IDC_spend_mini.tsv")
  }

}
