
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;


public class TestHDFSClient {
    private static final String URI_HOST =  "hdfs://192.168.142.128:8020";
    private static final String USER = "hadoop";
    private Configuration configuration = new Configuration();

    /**
     * 测试创建文件夹
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        FileSystem fileSystem = FileSystem.get(new URI(URI_HOST), configuration, USER);
        fileSystem.mkdirs(new Path("/typ/java2"));
        fileSystem.close();
        System.out.println("Finished!");
    }

    /**
     * 文件上传测试
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        // 1.获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI(URI_HOST), configuration, USER);
        // 2.执行操作
        fileSystem.copyFromLocalFile(new Path("e:/a1.txt"), new Path("/typ/java2"));
        // 3.关闭资源
        fileSystem.close();
    }

    /**
     * 文件下载测试
     * @throws URISyntaxException
     * @throws IOException
     * @throws InterruptedException
     */
    @Test
    public void testCopyToLocalFile() throws URISyntaxException, IOException, InterruptedException {
        FileSystem fileSystem = FileSystem.get(new URI(URI_HOST), configuration, USER);
        fileSystem.copyToLocalFile(false, new Path("/typ/java/a1.txt"), new Path("F:/a2.txt"), true);
        fileSystem.close();
    }
}
