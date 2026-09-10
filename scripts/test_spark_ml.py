from pyspark.sql import SparkSession
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler

spark = (
    SparkSession.builder
    .appName("SparkMLSmokeTest")
    .getOrCreate()
)

data = [
    (1.0, 2.0, 0.0),
    (2.0, 1.0, 0.0),
    (8.0, 9.0, 1.0),
    (9.0, 8.0, 1.0),
]

df = spark.createDataFrame(
    data,
    ["feature1", "feature2", "label"]
)

assembler = VectorAssembler(
    inputCols=["feature1", "feature2"],
    outputCol="features"
)

prepared = assembler.transform(df)

rf = RandomForestClassifier(
    labelCol="label",
    featuresCol="features",
    numTrees=10,
    seed=42
)

model = rf.fit(prepared)

print("================================")
print("Spark ML Smoke Test PASSED")
print("Trees:", model.getNumTrees)
print("================================")

spark.stop()