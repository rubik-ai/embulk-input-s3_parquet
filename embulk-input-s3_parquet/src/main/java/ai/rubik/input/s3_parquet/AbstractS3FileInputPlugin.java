package ai.rubik.input.s3_parquet;

import ai.rubik.input.s3_parquet.explorer.S3NameOrderPrefixFileExplorer;
import ai.rubik.input.s3_parquet.utils.DateUtils;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.retry.PredefinedRetryPolicies;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.google.common.annotations.VisibleForTesting;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.embulk.config.*;
import ai.rubik.input.s3_parquet.explorer.S3SingleFileExplorer;
import ai.rubik.input.s3_parquet.explorer.S3TimeOrderPrefixFileExplorer;
import org.embulk.spi.BufferAllocator;
import org.embulk.spi.Exec;
import org.embulk.spi.FileInputPlugin;
import org.embulk.spi.TransactionalFileInput;
import org.embulk.spi.util.InputStreamFileInput;
import org.embulk.spi.util.InputStreamFileInput.InputStreamWithHints;
import org.embulk.spi.util.ResumableInputStream;
import org.embulk.spi.util.RetryExecutor;
import ai.rubik.util.aws.credentials.AwsCredentials;
import ai.rubik.util.aws.credentials.AwsCredentialsTask;
import org.slf4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.*;

import static java.lang.String.format;
import static org.embulk.spi.util.RetryExecutor.retryExecutor;

public abstract class AbstractS3FileInputPlugin
        implements FileInputPlugin {
    private static final Logger LOGGER = Exec.getLogger(S3FileInputPlugin.class);
    private static final String FULL_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'";

    public interface PluginTask
            extends AwsCredentialsTask, FileList.Task, RetrySupportPluginTask, Task {
        @Config("bucket")
        String getBucket();

        @Config("path_prefix")
        @ConfigDefault("null")
        Optional<String> getPathPrefix();

        @Config("path")
        @ConfigDefault("null")
        Optional<String> getPath();

        @Config("last_path")
        @ConfigDefault("null")
        Optional<String> getLastPath();

        @Config("access_key_id")
        @ConfigDefault("null")
        Optional<String> getAccessKeyId();

        @Config("endpoint")
        @ConfigDefault("null")
        Optional<String> getEndpoint();

        @Config("http_proxy")
        @ConfigDefault("null")
        Optional<HttpProxy> getHttpProxy();

        void setHttpProxy(Optional<HttpProxy> httpProxy);

        @Config("incremental")
        @ConfigDefault("true")
        boolean getIncremental();

        @Config("skip_glacier_objects")
        @ConfigDefault("false")
        boolean getSkipGlacierObjects();

        @Config("use_modified_time")
        @ConfigDefault("false")
        boolean getUseModifiedTime();

        @Config("last_modified_time")
        @ConfigDefault("null")
        Optional<String> getLastModifiedTime();

        // TODO timeout, ssl, etc

        ////////////////////////////////////////
        // Internal configurations
        ////////////////////////////////////////

        FileList getFiles();

        void setFiles(FileList files);

        /**
         * end_modified_time is conditionally set if modified_time mode is enabled.
         * <p>
         * It is internal state and must not be set in config.yml
         */
        @Config("__end_modified_time")
        @ConfigDefault("null")
        Optional<Date> getEndModifiedTime();

        void setEndModifiedTime(Optional<Date> endModifiedTime);

        @ConfigInject
        BufferAllocator getBufferAllocator();
    }

    protected abstract Class<? extends PluginTask> getTaskClass();

    @Override
    public ConfigDiff transaction(ConfigSource config, FileInputPlugin.Control control) {
        PluginTask task = config.loadConfig(getTaskClass());

        errorIfInternalParamsAreSet(task);
        validateInputTask(task);
        // list files recursively
        task.setFiles(listFiles(task));

        // number of processors is same with number of files
        return resume(task.dump(), task.getFiles().getTaskCount(), control);
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
                             int taskCount,
                             FileInputPlugin.Control control) {
        PluginTask task = taskSource.loadTask(getTaskClass());

        // validate task
        newS3Client(task);

        control.run(taskSource, taskCount);

        // build next config
        ConfigDiff configDiff = Exec.newConfigDiff();

        // last_path
        if (task.getIncremental()) {
            if (task.getUseModifiedTime()) {
                Date endModifiedTime = task.getEndModifiedTime().orElse(new Date());
                configDiff.set("last_modified_time", new SimpleDateFormat(FULL_DATE_FORMAT).format(endModifiedTime));
            } else {
                Optional<String> lastPath = task.getFiles().getLastPath(task.getLastPath());
                LOGGER.info("Incremental job, setting last_path to [{}]", lastPath.orElse(""));
                configDiff.set("last_path", lastPath);
            }
        }
        return configDiff;
    }

    @Override
    public void cleanup(TaskSource taskSource,
                        int taskCount,
                        List<TaskReport> successTaskReports) {
        // do nothing
    }

    /**
     * Provide an overridable default client.
     * Since this returns an immutable object, it is not for any further customizations by mutating,
     * e.g., {@link AmazonS3#setEndpoint} will throw a runtime {@link UnsupportedOperationException}
     * Subclass's customization should be done through {@link AbstractS3FileInputPlugin#defaultS3ClientBuilder}.
     *
     * @param task Embulk plugin task
     * @return AmazonS3
     */
    protected AmazonS3 newS3Client(PluginTask task) {
        return defaultS3ClientBuilder(task).build();
    }

    /**
     * A base builder for the subclasses to then customize.builder
     *
     * @param task Embulk plugin
     * @return AmazonS3 client b
     **/
    protected AmazonS3ClientBuilder defaultS3ClientBuilder(PluginTask task) {
        return AmazonS3ClientBuilder
                .standard()
                .withCredentials(getCredentialsProvider(task))
                .withClientConfiguration(getClientConfiguration(task));
    }

    protected AWSCredentialsProvider getCredentialsProvider(PluginTask task) {
        return AwsCredentials.getAWSCredentialsProvider(task);
    }

    protected ClientConfiguration getClientConfiguration(PluginTask task) {
        ClientConfiguration clientConfig = new ClientConfiguration();

        //clientConfig.setProtocol(Protocol.HTTP);
        clientConfig.setMaxConnections(100); // SDK default: 50
//        clientConfig.setMaxErrorRetry(3); // SDK default: 3
        clientConfig.setSocketTimeout(8 * 60 * 1000); // SDK default: 50*1000
        clientConfig.setRetryPolicy(PredefinedRetryPolicies.NO_RETRY_POLICY);
        // set http proxy
        if (task.getHttpProxy().isPresent()) {
            setHttpProxyInAwsClient(clientConfig, task.getHttpProxy().get());
        }

        return clientConfig;
    }

    private void setHttpProxyInAwsClient(ClientConfiguration clientConfig, HttpProxy httpProxy) {
        // host
        clientConfig.setProxyHost(httpProxy.getHost());

        // port
        if (httpProxy.getPort().isPresent()) {
            clientConfig.setProxyPort(httpProxy.getPort().get());
        }

        // https
        clientConfig.setProtocol(httpProxy.getHttps() ? Protocol.HTTPS : Protocol.HTTP);

        // user
        if (httpProxy.getUser().isPresent()) {
            clientConfig.setProxyUsername(httpProxy.getUser().get());
        }

        // password
        if (httpProxy.getPassword().isPresent()) {
            clientConfig.setProxyPassword(httpProxy.getPassword().get());
        }
    }

    /**
     * Build the common retry executor from some configuration params of plugin task.
     *
     * @param task Plugin task.
     * @return RetryExecutor object
     */
    private static RetryExecutor retryExecutorFrom(RetrySupportPluginTask task) {
        return retryExecutor()
                .withRetryLimit(task.getMaximumRetries())
                .withInitialRetryWait(task.getInitialRetryIntervalMillis())
                .withMaxRetryWait(task.getMaximumRetryIntervalMillis());
    }

    private FileList listFiles(final PluginTask task) {
        try {
            AmazonS3 client = newS3Client(task);
            String bucketName = task.getBucket();
            FileList.Builder builder = new FileList.Builder(task);
            RetryExecutor retryExec = retryExecutorFrom(task);

            if (task.getPath().isPresent()) {
                LOGGER.info("Start getting object with path: [{}]", task.getPath().get());
                new S3SingleFileExplorer(bucketName, client, retryExec, task.getPath().get()).addToBuilder(builder);
                return builder.build();
            }

            // does not need to verify existent path prefix here since there is the validation requires either path or path_prefix
            LOGGER.info("Start listing file with prefix [{}]", task.getPathPrefix().get());
            if (task.getPathPrefix().get().equals("/")) {
                LOGGER.info("Listing files with prefix \"/\". This doesn't mean all files in a bucket. If you intend to read all files, use \"path_prefix: ''\" (empty string) instead.");
            }

            if (task.getUseModifiedTime()) {
                Date now = new Date();
                Optional<Date> from = task.getLastModifiedTime().isPresent()
                        ? Optional.of(DateUtils.parse(task.getLastModifiedTime().get(), Collections.singletonList(FULL_DATE_FORMAT)))
                        : Optional.empty();
                task.setEndModifiedTime(Optional.of(now));

                new S3TimeOrderPrefixFileExplorer(bucketName, client, retryExec, task.getPathPrefix().get(),
                        task.getSkipGlacierObjects(), from, now).addToBuilder(builder);
            } else {
                new S3NameOrderPrefixFileExplorer(bucketName, client, retryExec, task.getPathPrefix().get(),
                        task.getSkipGlacierObjects(), task.getLastPath().orElse(null)).addToBuilder(builder);
            }

            LOGGER.info("Found total [{}] files", builder.size());
            return builder.build();
        } catch (AmazonServiceException ex) {
            if (ex.getErrorType().equals(AmazonServiceException.ErrorType.Client)) {
                // HTTP 40x errors. auth error, bucket doesn't exist, etc. See AWS document for the full list:
                // http://docs.aws.amazon.com/AmazonS3/latest/API/ErrorResponses.html
                if (ex.getStatusCode() != 400   // 404 Bad Request is unexpected error
                        || "ExpiredToken".equalsIgnoreCase(ex.getErrorCode())) { // if statusCode == 400 && errorCode == ExpiredToken => throws ConfigException
                    throw new ConfigException(ex);
                }
            }
            throw ex;
        }
    }

    private void validateInputTask(final PluginTask task) {
        if (!task.getPathPrefix().isPresent() && !task.getPath().isPresent()) {
            throw new ConfigException("Either path or path_prefix is required");
        }
    }

    @Override
    public TransactionalFileInput open(TaskSource taskSource, int taskIndex) {
        PluginTask task = taskSource.loadTask(getTaskClass());
        return new S3FileInput(task, taskIndex);
    }

    @VisibleForTesting
    static class S3InputStreamReopener
            implements ResumableInputStream.Reopener {
        private final Logger log = Exec.getLogger(S3InputStreamReopener.class);
        private final String bucket;
        private final GetObjectRequest request;
        private final RetryExecutor retryExec;
        private final String endpoint;
        private final String accessKey;
        private final String secretKey;
        private final long contentLength;
        private final String key;

        public S3InputStreamReopener(String bucket, final String endpoint, final String accessKey, final String secretKey, final String key, long contentLength, GetObjectRequest request) {
            this(bucket, endpoint, accessKey, secretKey, key, contentLength, request, null);
        }

        public S3InputStreamReopener(String bucket, final String endpoint, final String accessKey, final String secretKey, final String key, long contentLength, GetObjectRequest request, RetryExecutor retryExec) {
            this.bucket = bucket;
            this.endpoint = endpoint;
            this.accessKey = accessKey;
            this.secretKey = secretKey;
            this.key = key;
            this.contentLength = contentLength;
            this.retryExec = retryExec;
            this.request = request;
        }

        @Override
        public InputStream reopen(final long offset, final Exception closedCause) throws IOException {
            log.warn(format("S3 read failed. Retrying GET request with %,d bytes offset", offset), closedCause);
            request.setRange(offset, contentLength - 1);  // [first, last]

            return new DefaultRetryable<InputStream>(format("Getting object '%s'", request.getKey())) {
                @Override
                public InputStream call() {
                    InputStream inputStream = null;
                    Configuration conf = new Configuration();
                    conf.set("fs.s3a.endpoint", endpoint);
                    conf.set("fs.s3a.access.key", accessKey);
                    conf.set("fs.s3a.secret.key", secretKey);
                    String path = String.format("s3a://%s/%s", bucket, key);
                    long length = 0;

                    ParquetReader reader = null;
                    List<GenericRecord> records = new ArrayList<>();
                    try {
                        reader = AvroParquetReader.builder(new Path(path)).withConf(conf).build();
                        Object obj = reader.read();

                        while (obj != null) {
                            if (obj instanceof GenericRecord) {
                                records.add(((GenericRecord) obj));
                            }
                            obj = reader.read();
                        }
                        Schema schema = records.get(0).getSchema();
                        ByteArrayOutputStream baos = new ByteArrayOutputStream();
                        DatumWriter<GenericRecord> writer = new GenericDatumWriter(schema);
                        JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, baos);
                        for (GenericRecord genericRecord : records) {
                            writer.write(genericRecord, encoder);
                        }
                        length = baos.toByteArray().length;
                        LOGGER.info("file content length is {}", length);
                        inputStream = new ByteArrayInputStream(baos.toByteArray());
                        encoder.flush();
                        baos.flush();

                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        if (reader != null) {
                            try {
                                reader.close();
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }
                    return inputStream;
                }
            }.executeWithCheckedException(retryExec, IOException.class);
        }
    }

    public class S3FileInput
            extends InputStreamFileInput
            implements TransactionalFileInput {
        public S3FileInput(PluginTask task, int taskIndex) {
            super(task.getBufferAllocator(), new SingleFileProvider(task, taskIndex));
        }

        public void abort() {
        }

        public TaskReport commit() {
            return Exec.newTaskReport();
        }

        @Override
        public void close() {
        }
    }

    @VisibleForTesting
    static void errorIfInternalParamsAreSet(PluginTask task) {
        if (task.getEndModifiedTime().isPresent()) {
            throw new ConfigException("'__end_modified_time' must not be set.");
        }
    }

    // TODO create single-file InputStreamFileInput utility
    private class SingleFileProvider
            implements InputStreamFileInput.Provider {
        private AmazonS3 client;
        private final String bucket;
        private final Iterator<String> iterator;
        private final RetryExecutor retryExec;
        private final String endpoint;
        private final String accessKey;
        private final String secretKey;

        public SingleFileProvider(PluginTask task, int taskIndex) {
            this.client = newS3Client(task);
            this.bucket = task.getBucket();
            this.iterator = task.getFiles().get(taskIndex).iterator();
            this.endpoint = task.getEndpoint().get();
            this.accessKey = task.getAccessKeyId().get();
            this.secretKey = task.getSecretAccessKey().get();

            this.retryExec = retryExecutorFrom(task);
        }

        @Override
        public InputStreamWithHints openNextWithHints() throws IOException {
            if (!iterator.hasNext()) {
                return null;
            }
            final String key = iterator.next();

            final GetObjectRequest request = new GetObjectRequest(bucket, key);

            InputStream inputStream = null;
            Configuration conf = new Configuration();
            conf.set("fs.s3a.endpoint", endpoint);
            conf.set("fs.s3a.access.key", accessKey);
            conf.set("fs.s3a.secret.key", secretKey);
            String path = String.format("s3a://%s/%s", bucket, key);
            long length = 0;

            ParquetReader reader = null;
            List<GenericRecord> records = new ArrayList<>();
            Schema schema;
            try {
                reader = AvroParquetReader.builder(new Path(path)).withConf(conf).build();
                Object obj = reader.read();

                while (obj != null) {
                    if (obj instanceof GenericRecord) {
                        records.add(((GenericRecord) obj));
                    }
                    obj = reader.read();
                }
                if (records.size() == 0) {
                    schema = SchemaBuilder.record("default") // source's name
                            .namespace("default") // source's namespace
                            .fields() // empty fields
                            .endRecord();
                } else {
                    schema = records.get(0).getSchema();
                }
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DatumWriter<GenericRecord> writer = new GenericDatumWriter(schema);
                JsonEncoder encoder = EncoderFactory.get().jsonEncoder(schema, baos);
                for (GenericRecord genericRecord : records) {
                    writer.write(genericRecord, encoder);
                }
                length = baos.toByteArray().length;
                LOGGER.info("file content length is {}", length);
                inputStream = new ByteArrayInputStream(baos.toByteArray());
                encoder.flush();
                baos.flush();

            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                if (reader != null) {
                    try {
                        reader.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
            InputStream finalStream = new ResumableInputStream(inputStream, new S3InputStreamReopener(bucket, endpoint, accessKey, secretKey, key, length, request, retryExec));
            return new InputStreamWithHints(finalStream, path);
        }

        @Override
        public void close() throws IOException {

        }
    }
}
