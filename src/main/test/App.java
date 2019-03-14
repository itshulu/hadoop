import com.sl.hdfs.HdfsClient;
import org.junit.Test;

import java.io.IOException;

public class App {
    @Test
    public void downloadTest() throws IOException {
        HdfsClient hdfsClient = new HdfsClient();
        hdfsClient.download();
    }
}
