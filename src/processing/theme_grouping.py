from pyspark.ml.clustering import KMeans

def assign_themes(df, k=5):
    kmeans = KMeans(
        featuresCol="semantic_features",
        predictionCol="theme_id",
        k=k,
        seed=42
    )

    model = kmeans.fit(df)
    return model.transform(df)