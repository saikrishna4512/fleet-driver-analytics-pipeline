# ==========================================
# SILVER LAYER - DATA CLEANING & STANDARDIZATION
# ==========================================

# Read Bronze table
bronze_df = spark.table("bronze_vehicle_tracking")

# Show sample data
bronze_df.show(5)

# Count records
bronze_df.count()

# Import required PySpark functions
from pyspark.sql.functions import col, trim, lower, to_timestamp

# Start Silver cleaning from Bronze data
silver_df = bronze_df

# Remove completely null rows
silver_df = silver_df.dropna(how="all")

# Remove accidental duplicate header rows if present in the data
silver_df = silver_df.filter(col("VehicleId").isNotNull())

# Standardize text columns by trimming spaces
silver_df = silver_df.withColumn("VehicleNo", trim(col("VehicleNo"))) \
                     .withColumn("drivername", trim(col("drivername"))) \
                     .withColumn("Ignition", trim(col("Ignition"))) \
                     .withColumn("GPSstatus", trim(col("GPSstatus"))) \
                     .withColumn("LocationName", trim(col("LocationName")))

# Display cleaned sample records
silver_df.show(5)

# ==========================================
# SILVER STEP 2 - DEDUPLICATION & TYPE CLEANUP
# ==========================================

from pyspark.sql.functions import col, to_timestamp

# Remove duplicate GPS records
# Deduplication rule: same vehicle at the same timestamp should appear only once
silver_df = silver_df.dropDuplicates(["VehicleId", "DateTime"])

# Ensure key numeric fields are in the correct format
silver_df = silver_df.withColumn("Latitude", col("Latitude").cast("double")) \
                     .withColumn("Longitude", col("Longitude").cast("double")) \
                     .withColumn("speed", col("speed").cast("double")) \
                     .withColumn("DateTime", to_timestamp(col("DateTime")))

# Keep only valid GPS records
silver_df = silver_df.filter(
    col("Latitude").isNotNull() &
    col("Longitude").isNotNull() &
    col("DateTime").isNotNull()
)

# Validate cleaned data
silver_df.printSchema()
silver_df.count()

# ==========================================
# SILVER STEP 3 - FEATURE ENGINEERING
# ==========================================

from pyspark.sql.functions import col, hour, dayofweek, when

# Extract time-based features
silver_df = silver_df.withColumn("hour", hour(col("DateTime"))) \
                     .withColumn("day_of_week", dayofweek(col("DateTime")))

# Create speed category (business logic)
silver_df = silver_df.withColumn(
    "speed_category",
    when(col("speed") == 0, "Stationary")
    .when((col("speed") > 0) & (col("speed") <= 20), "Slow")
    .when((col("speed") > 20) & (col("speed") <= 60), "Normal")
    .otherwise("Overspeed")
)

# Preview transformed data
silver_df.select("VehicleNo", "speed", "speed_category", "hour", "day_of_week").show(10)

# ==========================================
# SILVER STEP 4 - DISTANCE CALCULATION (HAVERSINE)
# ==========================================

from pyspark.sql.window import Window
from pyspark.sql.functions import lag, radians, sin, cos, sqrt, atan2

# Create window partition (per vehicle ordered by time)
window_spec = Window.partitionBy("VehicleId").orderBy("DateTime")

# Get previous coordinates
silver_df = silver_df.withColumn("prev_lat", lag("Latitude").over(window_spec)) \
                     .withColumn("prev_lon", lag("Longitude").over(window_spec))

# Earth radius in kilometers
R = 6371

# Apply Haversine formula
silver_df = silver_df.withColumn("lat1", radians(col("prev_lat"))) \
                     .withColumn("lon1", radians(col("prev_lon"))) \
                     .withColumn("lat2", radians(col("Latitude"))) \
                     .withColumn("lon2", radians(col("Longitude"))) \
                     .withColumn("dlat", col("lat2") - col("lat1")) \
                     .withColumn("dlon", col("lon2") - col("lon1")) \
                     .withColumn("a", sin(col("dlat")/2)**2 + cos(col("lat1")) * cos(col("lat2")) * sin(col("dlon")/2)**2) \
                     .withColumn("c", 2 * atan2(sqrt(col("a")), sqrt(1 - col("a")))) \
                     .withColumn("distance_km", R * col("c"))

# Replace null distance (first record per vehicle) with 0
silver_df = silver_df.fillna({"distance_km": 0})

# Preview results
silver_df.select("VehicleId", "DateTime", "distance_km").show(10)

# ==========================================
# SILVER STEP 5 - DRIVER BEHAVIOUR DETECTION
# ==========================================

from pyspark.sql.functions import lag
from pyspark.sql.window import Window

# Window for speed comparison
window_spec = Window.partitionBy("VehicleId").orderBy("DateTime")

# Get previous speed
silver_df = silver_df.withColumn("prev_speed", lag("speed").over(window_spec))

# Overspeed flag (> 60 km/h)
silver_df = silver_df.withColumn(
    "overspeed_flag",
    when(col("speed") > 60, 1).otherwise(0)
)

# Harsh braking (drop > 20)
silver_df = silver_df.withColumn(
    "harsh_brake_flag",
    when((col("prev_speed") - col("speed")) > 20, 1).otherwise(0)
)

# Harsh acceleration (increase > 20)
silver_df = silver_df.withColumn(
    "harsh_acc_flag",
    when((col("speed") - col("prev_speed")) > 20, 1).otherwise(0)
)

# Idle (engine ON + speed = 0)
silver_df = silver_df.withColumn(
    "idle_flag",
    when((col("Ignition") == "On") & (col("speed") == 0), 1).otherwise(0)
)

# Preview behaviour flags
silver_df.select(
    "VehicleId", "speed", "prev_speed",
    "overspeed_flag", "harsh_brake_flag",
    "harsh_acc_flag", "idle_flag"
).show(10)

# ==========================================
# SILVER STEP 6 - SAVE SILVER DELTA TABLE
# ==========================================

# Save the cleaned and enriched dataset as a Delta table
# This Silver table will be used for Gold-level business aggregations
silver_df.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_vehicle_tracking")

# Validate Silver table
spark.table("silver_vehicle_tracking").count()

spark.table("silver_vehicle_tracking").select(
    "VehicleId", "drivername", "DateTime", "speed",
    "speed_category", "distance_km",
    "overspeed_flag", "harsh_brake_flag",
    "harsh_acc_flag", "idle_flag"
).show(10)

