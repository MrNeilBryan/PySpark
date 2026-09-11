from pyspark.sql import functions as F

# --- Drill 1: create a DataFrame ---
data = [
    ("Alice", "Savings",  1200.00),
    ("Bob",   "Checking",  350.00),
    ("Carol", "Savings",  8000.00),
    ("Dave",  "Current",  2750.00),
    ("Eve",   "Savings",   150.00),
]

columns = ["name", "account_type", "balance"]

df = spark.createDataFrame(data, columns)

df.show()            # print the table
# df.printSchema()   # show column names + types
# df.count()         # count rows (returns a number)

# --- Summing a column ---
df.agg(F.sum("balance")).show()                    # total of all balances

# --- Sum with a WHERE clause ---
(df.filter(F.col("balance") > 1000.00)
   .agg(F.sum("balance"))
   .show())                                        # total of balances over 1000

# --- Sum with two conditions (AND) ---
(df.filter((F.col("balance") > 1000.00) & (F.col("account_type") == "Savings"))
   .agg(F.sum("balance"))
   .show())                                        # Savings balances over 1000

# --- Filter rows where the name contains a lowercase "e" ---
(df.filter((F.col("balance") > 1000.00) & (F.col("name").contains("e")))
   .agg(F.sum("balance"))
   .show())                                        # over-1000 with an "e" in the name

# --- Just show matching rows instead of summing ---
(df.filter(F.col("name").contains("e"))
   .show())                                        # people with an "e" in their name
