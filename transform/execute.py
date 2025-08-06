import sys
import os
import time
from pyspark.sql import SparkSession
from pyspark.sql import types as T 
from pyspark.sql import functions as F
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from utility.utility import setup_logging, format_time

def create_spark_session(logger):
    """Initialize Spark session with logging."""
    logger.debug("Initializing Spark Session")
    return SparkSession.builder.appName("SpotifyDataTransfer").getOrCreate()

def load_and_clean(logger, spark, input_dir, output_dir):
    """Stage 1: Load data, drop duplicates, remove nulls, save cleaned data."""
    try:
        logger.info("Starting Stage 1: Data loading and cleaning")
        
        # Define schemas
        artists_schema = T.StructType([
            T.StructField("id", T.StringType(), False),
            T.StructField("followers", T.FloatType(), True),
            T.StructField("genres", T.StringType(), True),
            T.StructField("name", T.StringType(), True),
            T.StructField("popularity", T.IntegerType(), True),
        ])

        recommendations_schema = T.StructType([
            T.StructField("id", T.StringType(), False),
            T.StructField("related_ids", T.ArrayType(T.StringType()), True),
        ])

        tracks_schema = T.StructType([
            T.StructField("id", T.StringType(), False),
            T.StructField("name", T.StringType(), True),
            T.StructField("popularity", T.IntegerType(), True),
            T.StructField("duration_ms", T.IntegerType(), True),
            T.StructField("explicit", T.IntegerType(), True),
            T.StructField("artists", T.StringType(), True),
            T.StructField("id_artists", T.StringType(), True),
            T.StructField("release_date", T.StringType(), True),
            T.StructField("danceability", T.FloatType(), True),
            T.StructField("energy", T.FloatType(), True),
            T.StructField("key", T.IntegerType(), True),
            T.StructField("loudness", T.FloatType(), True),
            T.StructField("mode", T.IntegerType(), True),  
            T.StructField("speechiness", T.FloatType(), True),
            T.StructField("acousticness", T.FloatType(), True),  
            T.StructField("instrumentalness", T.FloatType(), True),
            T.StructField("liveness", T.IntegerType(), True),  
            T.StructField("valence", T.FloatType(), True),  
            T.StructField("tempo", T.FloatType(), True),
            T.StructField("time_signature", T.IntegerType(), True),  
        ])

        # Load data
        logger.debug("Loading artists data")
        artists_df = spark.read.schema(artists_schema).csv(os.path.join(input_dir, "artists.csv"), header=True)
        
        logger.debug("Loading recommendations data")
        recommendation_df = spark.read.schema(recommendations_schema).json(os.path.join(input_dir, "fixed_da.json"))
        
        logger.debug("Loading tracks data")
        tracks_df = spark.read.schema(tracks_schema).csv(os.path.join(input_dir, "tracks.csv"), header=True)

        # Clean data
        logger.debug("Cleaning artists data")
        artists_df = artists_df.dropDuplicates(["id"]).filter(F.col("id").isNotNull())
        
        logger.debug("Cleaning recommendations data")
        recommendation_df = recommendation_df.dropDuplicates(["id"]).filter(F.col("id").isNotNull())
        
        logger.debug("Cleaning tracks data")
        tracks_df = tracks_df.dropDuplicates(["id"]).filter(F.col("id").isNotNull())

        # Save cleaned data
        logger.info("Saving cleaned data")
        artists_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "artists"))
        recommendation_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "recommendations"))
        tracks_df.write.mode("overwrite").parquet(os.path.join(output_dir, "stage1", "tracks"))

        logger.info("Stage 1 completed: Cleaned data saved")
        return artists_df, recommendation_df, tracks_df

    except Exception as e:
        logger.error(f"Error in Stage 1: {e}")
        raise

def create_master_table(logger, output_dir, artists_df, recommendations_df, tracks_df):
    """Stage 2: Create master table by joining artists, tracks, and recommendations."""
    try:
        logger.info("Starting Stage 2: Creating master table")
        
        logger.debug("Preparing tracks data")
        tracks_df = tracks_df.withColumn("id_artists_array", F.from_json(F.col("id_artists"), F.ArrayType(F.StringType())))
        tracks_exploded = tracks_df.select("id", "name", "popularity", "id_artists_array").withColumn("artist_id", F.explode("id_artists_array"))

        logger.debug("Joining tracks with artists")
        master_df = tracks_exploded.join(artists_df, tracks_exploded.artist_id == artists_df.id, "left") \
                                .select(
                                    tracks_exploded.id.alias("track_id"),
                                    tracks_exploded.name.alias("track_name"),
                                    tracks_exploded.popularity.alias("track_popularity"),
                                    artists_df.id.alias("artist_id"),
                                    artists_df.name.alias("artist_name"),
                                    artists_df.followers,
                                    artists_df.genres,
                                    artists_df.popularity.alias("artist_popularity")
                                )
        
        logger.debug("Joining with recommendations")
        master_df = master_df.join(recommendations_df, master_df.artist_id == recommendations_df.id, "left") \
                                .select(
                                    master_df.track_id,
                                    master_df.track_name,
                                    master_df.track_popularity,
                                    master_df.artist_id,
                                    master_df.artist_name,
                                    master_df.followers,
                                    master_df.genres,
                                    master_df.artist_popularity,
                                    recommendations_df.related_ids
                                )
        
        logger.info("Saving master table")
        master_df.write.mode("overwrite").parquet(os.path.join(output_dir, "Stage2", "master_table"))
        logger.info("Stage 2 completed: Master table saved")

    except Exception as e:
        logger.error(f"Error in Stage 2: {e}")
        raise

def create_query_tables(logger, output_dir, artists_df, recommendations_df, tracks_df):
    """Stage 3: Create query-optimized tables."""
    try:
        logger.info("Starting Stage 3: Creating query-optimized tables")
        
        logger.debug("Creating exploded recommendations table")
        recommendations_exploded = recommendations_df.withColumn("related_id", F.explode("related_ids")) \
                                                    .select("id", "related_id")
        recommendations_exploded.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "recommendations_exploded"))

        logger.debug("Creating artist-track relationship table")
        tracks_exploded = (
            tracks_df
            .withColumn("id_artists_array", F.from_json(F.col("id_artists"), T.ArrayType(T.StringType())))
            .withColumn("artist_id", F.explode("id_artists_array"))
            .select("id", "artist_id")
        )
        tracks_exploded.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "artist_track"))

        logger.debug("Creating tracks metadata table")
        tracks_metadata = tracks_df.select(
            "id", "name", "popularity", "duration_ms", "danceability", "energy", "tempo"
        )
        tracks_metadata.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "track_metadata"))

        logger.debug("Creating artists metadata table")
        artists_metadata = artists_df.select("id", "name", "followers", "popularity")
        artists_metadata.write.mode("overwrite").parquet(os.path.join(output_dir, "stage3", "artists_metadata"))

        logger.info("Stage 3 completed: Query-optimized tables saved")

    except Exception as e:
        logger.error(f"Error in Stage 3: {e}")
        raise

if __name__ == "__main__":
    logger = setup_logging("transform.log")
    
    if len(sys.argv) != 3:
        logger.error("Usage: python script.py <input_dir> <output_dir>")
        sys.exit(1)

    try:
        start_time = time.time()
        input_dir = sys.argv[1]
        output_dir = sys.argv[2]

        logger.info("Starting transformation pipeline")
        spark = create_spark_session(logger)

        # Execute all stages
        artists_df, recommendations_df, tracks_df = load_and_clean(logger, spark, input_dir, output_dir)
        create_master_table(logger, output_dir, artists_df, recommendations_df, tracks_df)
        create_query_tables(logger, output_dir, artists_df, recommendations_df, tracks_df)

        end_time = time.time()
        logger.info("Transformation pipeline completed successfully")
        logger.info(f"Total transformation time: {format_time(end_time - start_time)}")

    except Exception as e:
        logger.error(f"Transformation pipeline failed: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.debug("Spark session stopped")