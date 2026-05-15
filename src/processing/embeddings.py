from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF, IDF

def add_tfidf_features(df):
    tokenizer = Tokenizer(
        inputCol="analysis_text",
        outputCol="tokens"
    )

    remover = StopWordsRemover(
        inputCol="tokens",
        outputCol="filtered_tokens"
    )

    hashing_tf = HashingTF(
        inputCol="filtered_tokens",
        outputCol="raw_features",
        numFeatures=2048
    )

    idf = IDF(
        inputCol="raw_features",
        outputCol="semantic_features"
    )

    tokenized = tokenizer.transform(df)
    cleaned = remover.transform(tokenized)
    featurized = hashing_tf.transform(cleaned)

    idf_model = idf.fit(featurized)
    return idf_model.transform(featurized)
