from delta.tables import DeltaTable
from pyspark.sql.functions import current_timestamp, lit

# 1. PATHS
source_path = "abfss://neb_workspace@onelake.dfs.fabric.microsoft.com/free-sql-db-5618880_Mirror.mountedrelationaldatabase/Tables/SalesLT/ProductDescription"
target_path = "abfss://neb_workspace@onelake.dfs.fabric.microsoft.com/neb_lakehouse.Lakehouse/Tables/Silver_Table"

# 2. READ SOURCE
df_source = spark.read.format("delta").load(source_path)

# 3. INITIALIZE TARGET
try:
    target_table = DeltaTable.forPath(spark, target_path)
except:
    # First time run: Create table with extra columns for tracking
    df_initial = df_source.withColumn("Fabric_Arrival_Time", current_timestamp()) \
                         .withColumn("IsDeleted", lit(False)) \
                         .withColumn("DeletedAt", lit(None).cast("timestamp"))
    
    df_initial.write.format("delta").mode("overwrite").save(target_path)
    target_table = DeltaTable.forPath(spark, target_path)

# 4. PERFORM MERGE WITH SOFT DELETE
(target_table.alias("target")
  .merge(
    df_source.alias("source"),
    "target.ProductDescriptionID = source.ProductDescriptionID"
  )
  .whenMatchedUpdate(
    # Update if data changed OR if a previously deleted record reappears
    condition = "target.Description != source.Description OR target.IsDeleted = true",
    set = {
        "Description": "source.Description",
        "Fabric_Arrival_Time": current_timestamp(),
        "IsDeleted": lit(False),
        "DeletedAt": lit(None)
    }
  )
  .whenNotMatchedInsert(
    values = {
        "ProductDescriptionID": "source.ProductDescriptionID",
        "Description": "source.Description",
        "Fabric_Arrival_Time": current_timestamp(),
        "IsDeleted": lit(False),
        "DeletedAt": lit(None)
    }
  )
  # THIS RECORD THE DELETE:
  .whenNotMatchedBySourceUpdate(
    set = {
        "IsDeleted": lit(True),
        "DeletedAt": current_timestamp()
    }
  )
  .execute()
)

print("Soft Sync complete: Deletes are now flagged in Silver_Table.")
