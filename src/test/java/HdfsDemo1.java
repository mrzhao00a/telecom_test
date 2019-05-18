import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;


public class HdfsDemo1 {

    FileSystem fs =null;
    @Before
    public void init() throws Exception{
        fs = FileSystem.newInstance(new URI("hdfs://172.16.1.1:8020"), new Configuration(), "rendadev");//9000会报错
    }

    @Test
    public void mkdirs() throws Exception{
        fs.mkdirs(new Path("/bb"));
    }
/**
 *     上传
 *             下载
 *     新建文件
 *             详细列表
 *     删除
 */
    @Test
    public void update() throws Exception{
        fs.copyFromLocalFile(new Path("D://a.txt"),new Path("/d.txt"));
    }

    @Test
    public void download() throws Exception{
        fs.copyToLocalFile(false,new Path("/d.txt"),new Path("D://test//e.txt"),true);
    }

    @Test
    public void newFile() throws Exception{
        fs.createNewFile(new Path("/aa.txt"));
    }

    @Test
    public void delete() throws Exception{
        fs.delete(new Path("/aa.txt"),true);
    }

    @Test
    public void listFiles() throws Exception{
        FileStatus[] fileStatuses = fs.listStatus(new Path("/"));
        for (FileStatus f :fileStatuses) {
            System.out.println(f.getPath()+"\t"+f.getBlockSize()+"\t"+f.getLen());
        }
        /**
         * hdfs://172.16.1.1:8020/apps	0	0
         * hdfs://172.16.1.1:8020/cluster	0	0
         * hdfs://172.16.1.1:8020/data	0	0
         * hdfs://172.16.1.1:8020/data2	0	0
         * hdfs://172.16.1.1:8020/fac_run	0	0
         * hdfs://172.16.1.1:8020/hbase	0	0
         */
    }



}
