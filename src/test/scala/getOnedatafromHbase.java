import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;


public class getOnedatafromHbase {
    static Connection getcontent() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "tjnw-bg-pe-02,tjnw-bg-pe-04,tjnw-bg-pe-05");
        Connection connection = ConnectionFactory.createConnection(configuration);

        return connection;
    }


    /**
     * 查询某一条数据的字段组成
     *
     * @param tablename 表名
     * @param row       rowkey
     * @throws IOException
     */
    public static void getTableDesc(String tablename, String row) throws IOException {
        Connection conection = getOnedatafromHbase.getcontent();
        Table nm_news = conection.getTable(TableName.valueOf(tablename));
        String rowkey = row; //"1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05";
        Get get = new Get(Bytes.toBytes(rowkey));
        Result result = nm_news.get(get);
//        System.out.println(result);
        //keyvalues={1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:articleId/1557879485136/Put/vlen=32/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:bloggerId/1557879485136/Put/vlen=10/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:commentNum/1557879485136/Put/vlen=1/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:content/1557879485136/Put/vlen=161/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:crawlDate/1557879485136/Put/vlen=13/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:createDate/1557879485136/Put/vlen=13/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:dataSource/1557879485136/Put/vlen=2/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:enabled/1557879485136/Put/vlen=1/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:formerId/1557879485136/Put/vlen=0/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:forwardNum/1557879485136/Put/vlen=1/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:headPicUrl/1557879485136/Put/vlen=0/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:hotValue/1557879485136/Put/vlen=3/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:htmlcontent/1557879485136/Put/vlen=161/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:isContainsImg/1557879485136/Put/vlen=1/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:isForward/1557879485136/Put/vlen=1/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:isSensitive/1557879485136/Put/vlen=1/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:languageType/1557879485136/Put/vlen=4/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:location2/1557879485136/Put/vlen=6/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:location3/1557879485136/Put/vlen=6/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:mediaName/1557879485136/Put/vlen=12/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:mediaType/1557879485136/Put/vlen=2/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:news_city/1557879485136/Put/vlen=6/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:news_province/1557879485136/Put/vlen=6/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:nickName/1557879485136/Put/vlen=12/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:origincontent/1557879485136/Put/vlen=0/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:originimgs/1557879485136/Put/vlen=0/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:originnickname/1557879485136/Put/vlen=0/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:originposttime/1557879485136/Put/vlen=13/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:originuid/1557879485136/Put/vlen=0/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:project/1557879485136/Put/vlen=8/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:publishDate/1557879485136/Put/vlen=13/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:readNum/1557879485136/Put/vlen=1/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:thumbsUpNum/1557879485136/Put/vlen=1/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:title/1557879485136/Put/vlen=161/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:topic/1557879485136/Put/vlen=9/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:udClassifies/1557879485136/Put/vlen=148/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:udClassify/1557879485136/Put/vlen=5/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:url/1557879485136/Put/vlen=60/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:wbIsMedia/1557879485136/Put/vlen=1/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/nlp:coefficientKeywords/1557879485136/Put/vlen=227/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/nlp:frequencyHotwords/1557879485136/Put/vlen=322/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/nlp:organization/1557879485136/Put/vlen=22/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/nlp:place/1557879485136/Put/vlen=13/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/nlp:sentmentialScore/1557879485136/Put/vlen=1/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/nlp:simhash/1557879485136/Put/vlen=64/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/nlp:summary/1557879485136/Put/vlen=143/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/nlp:time/1557879485136/Put/vlen=21/seqid=0, 1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/nlp:weightKeywords/1557879485136/Put/vlen=229/seqid=0}
//        CellScanner cellScanner = result.cellScanner();
//        while (cellScanner.advance()){
//            Cell current = cellScanner.current();
//            String s = Bytes.toString(current.getQualifierArray());
//            System.out.println(s);
//        }
        List<Cell> cells = result.listCells();
//        List<String> list = new ArrayList<>();
//        for (Cell cell : cells) {
//            String[] split = cell.toString().split("/");
//            list.add(split[1]);
//        }
//       list.forEach(s-> System.out.println(s));
        cells.stream().forEach(s->{
            byte[] clone = s.getValue().clone();
            System.out.println(Bytes.toString(clone));
        });
    }
    public static void scanTable(String tablename,String startKey,String stopKey) throws IOException {
        Connection conection = getOnedatafromHbase.getcontent();
        Table nm_news = conection.getTable(TableName.valueOf(tablename));
        Scan scan = new Scan(Bytes.toBytes(startKey),Bytes.toBytes(stopKey));
        ResultScanner scanner = nm_news.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();

        while (iterator.hasNext()){
            Result result = iterator.next();
            List<Cell> cells = result.listCells();
            cells.stream().forEach(s->{
                byte[] value = s.getValue();
                System.out.println(Bytes.toString(value));
            });
//            cells.forEach(c->{
//                String s = c.getValueArray().toString();
//
//                System.out.println(s);
//            });
//            long count = cells.stream().count();
//            System.out.println(count);
//            cells.stream().filter(s-> StringUtils.isNotEmpty(s.toString())).forEach(s->{ });

//            Iterator<Cell>cs = cells.iterator();
//            while (cs.hasNext()){
//                Cell next = cs.next();
//                byte[] clone = next.getValue().clone();
//                String s = Bytes.toString(clone);
//
//                System.out.println(s);
//            }
        }


    }

    public static void main(String[] args) throws IOException {
//        scanTable("nm_news","1557802474000","1557802475000");
        String rowkey = "1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05";
        getTableDesc("nm_news",rowkey);
    }

}
