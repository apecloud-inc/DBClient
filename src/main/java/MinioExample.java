import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;
import io.minio.errors.MinioException;
import io.minio.messages.Bucket;

import java.io.IOException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.List;

public class MinioExample {
    public static void doTest() {
//        String endpoint = "http://127.0.0.1:9000";
//        String accessKey = "s6nrm2fq";
//        String secretKey = "s6nrm2fq";
//        String bucketName = "mybucket";
        String host = System.getenv("host");
        String port = System.getenv("port");
        String accessKey = System.getenv("accessKey");
        String secretKey = System.getenv("secretKey");
        String bucketName = System.getenv("bucketName");
        String endpoint = "http://" + host + ":" + port;
        try {
            //create MinioClient
            MinioClient minioClient =
                    MinioClient.builder()
                            .endpoint(endpoint)
                            .credentials(accessKey, secretKey)
                            .build();
            // check bucket is exist
            BucketExistsArgs bucketExistsArgs = BucketExistsArgs.builder().bucket(bucketName).build();
            boolean exists = minioClient.bucketExists(bucketExistsArgs);
            if (!exists) {
                // if it is not exist, create it
                MakeBucketArgs makeBucketArgs= MakeBucketArgs.builder().bucket(bucketName).build();
                minioClient.makeBucket(makeBucketArgs);
                System.out.println("Bucket " + bucketName + " created successfully");
            } else {
                System.out.println("Bucket " + bucketName + " already exists");
            }
            // list all buckets
            List<Bucket> bucketList = minioClient.listBuckets();
            for (Bucket bucket : bucketList) {
                System.out.println(bucket.creationDate() + ", " + bucket.name());
            }
        } catch (MinioException | IOException | NoSuchAlgorithmException | InvalidKeyException e) {
            System.out.println("Error occurred: " + e);
        }
    }
}
