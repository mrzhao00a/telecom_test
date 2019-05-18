public class splittest {
    public static void main(String[] args) {
        String line = "1557802474000-wb-E63E65DD5B53B90A8CC1682AE7B5CA05/base:articleId/1557879485136/Put/vlen=32/seqid=0";
        String[] splits = line.split("/");
        for (int i = 0 ; i<splits.length;i++){
            System.out.println(i+"-------"+splits[i]);

        }
    }
}
