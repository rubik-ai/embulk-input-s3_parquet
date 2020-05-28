package ai.rubik.input.s3_parquet.explorer;

import ai.rubik.input.s3_parquet.FileList;
import com.amazonaws.services.s3.AmazonS3;
import org.embulk.spi.util.RetryExecutor;

public abstract class S3FileExplorer
{
    protected String bucketName;
    protected AmazonS3 s3Client;
    protected RetryExecutor retryExecutor;

    public S3FileExplorer(final String bucketName, final AmazonS3 s3Client, final RetryExecutor retryExecutor)
    {
        this.bucketName = bucketName;
        this.s3Client = s3Client;
        this.retryExecutor = retryExecutor;
    }

    public abstract void addToBuilder(FileList.Builder builder);
}
