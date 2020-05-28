Embulk::JavaPlugin.register_input(
  :s3_parquet, "ai.rubik.input.s3_parquet.S3FileInputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
