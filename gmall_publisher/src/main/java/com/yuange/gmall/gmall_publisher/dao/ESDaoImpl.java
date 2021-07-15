package com.yuange.gmall.gmall_publisher.dao;

import com.alibaba.fastjson.JSONObject;
import com.yuange.gmall.gmall_publisher.beans.Option;
import com.yuange.gmall.gmall_publisher.beans.SaleDetail;
import com.yuange.gmall.gmall_publisher.beans.Stat;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.MetricAggregation;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * @作者：袁哥
 * @时间：2021/7/15 10:56
 */
@Repository
public class ESDaoImpl implements  ESDao {

    @Autowired //从容器中取一个JestClient类型的对象
    private JestClient jestClient;

    /*
            从ES中查询出数据
            date :   指定查询的index 名称
                            gmall2020_sale_detail +  date
            keyword: 全文检索的关键字
            startpage：
                    计算from :  (N - 1) size
                    1页 10 条数据。
                    startpage： 1    from: 0  size: 10
                    startpage： 2    from: 10 size 10
                    startpage: N     from: (N - 1) size
                    startpage: 3     from: 20 , size : 10
            size：   查询返回的数据条数
            查询条件：
                GET /gmall2020_sale_detail-query/_search
                {
                  "query": {
                    "match": {
                      "sku_name": "手机"
                    }
                  },
                  "from": 0,
                  "size": 20,
                  "aggs": {
                    "genderCount": {
                      "terms": {
                        "field": "user_gender",
                        "size": 10
                      }
                    },
                    "ageCount": {
                      "terms": {
                        "field": "user_age",
                        "size": 150
                      }
                    }
                  }
                }

     */
    public SearchResult getDataFromES(String date, Integer startpage, Integer size, String keyword) throws IOException {
        String indexName ="gmall_sale_detail" + date;
        int from = (startpage - 1 ) * size;

        // genderCount:{}
        TermsBuilder aggs1 = AggregationBuilders.terms("genderCount").field("user_gender").size(10);

        // "ageCount":{}
        TermsBuilder aggs2 = AggregationBuilders.terms("ageCount").field("user_age").size(150);

        // query":{}
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("sku_name", keyword);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(matchQueryBuilder).aggregation(aggs1).aggregation(aggs2);

        //生成查询的字符串
        Search search = new Search.Builder(searchSourceBuilder.toString()).addType("_doc").addIndex(indexName).build();
        SearchResult searchResult = jestClient.execute(search);
        return searchResult;
    }


    // 将ES中查询的数据，按照指定的格式，封装 detail数据
    public List<SaleDetail> getDetailData(SearchResult searchResult){

        ArrayList<SaleDetail> saleDetails = new ArrayList<>();

        List<SearchResult.Hit<SaleDetail, Void>> hits = searchResult.getHits(SaleDetail.class);

        for (SearchResult.Hit<SaleDetail, Void> hit : hits) {
            SaleDetail saleDetail = hit.source;
            saleDetail.setEs_metadata_id(hit.id);
            saleDetails.add(saleDetail);
        }
        return saleDetails;
    }

    //  将ES中查询的数据，按照指定的格式，封装 age相关的 stat数据
    public Stat getAgeStat(SearchResult searchResult){
        Stat stat = new Stat();
        MetricAggregation aggregations = searchResult.getAggregations();
        TermsAggregation ageCount = aggregations.getTermsAggregation("ageCount");
        List<TermsAggregation.Entry> buckets = ageCount.getBuckets();
        int agelt20=0;
        int agege30=0;
        int age20to30=0;
        double sumCount=0;
        for (TermsAggregation.Entry bucket : buckets) {
            if (Integer.parseInt(bucket.getKey()) < 20 ){
                agelt20 += bucket.getCount();
            }else if(Integer.parseInt(bucket.getKey()) >= 30){
                agege30 += bucket.getCount();
            }else{
                age20to30+=bucket.getCount();
            }
        }
        sumCount = age20to30 + agege30 + agelt20;

        DecimalFormat format = new DecimalFormat("###.00");

        List<Option> ageoptions =new ArrayList<>();

        double perlt20 = agelt20 / sumCount * 100;
        double per20to30 = age20to30 / sumCount * 100;

        ageoptions.add(new Option("20岁以下",Double.parseDouble(format.format(perlt20  ))));
        ageoptions.add(new Option("20岁到30岁",Double.parseDouble(format.format( per20to30))));
        ageoptions.add(new Option("30岁及30岁以上",Double.parseDouble(format.format(100 - perlt20 - per20to30  ))));

        stat.setOptions(ageoptions);
        stat.setTitle("用户年龄占比");
        return stat;
    }

    public Stat getGenderStat(SearchResult searchResult){
        Stat stat = new Stat();
        MetricAggregation aggregations = searchResult.getAggregations();

        TermsAggregation ageCount = aggregations.getTermsAggregation("genderCount");

        List<TermsAggregation.Entry> buckets = ageCount.getBuckets();

        int maleCount=0;
        int femaleCount=0;

        double sumCount=0;

        for (TermsAggregation.Entry bucket : buckets) {
            if (bucket.getKey().equals("F") ){
                femaleCount += bucket.getCount();
            }else{
                maleCount += bucket.getCount();
            }
        }
        sumCount = maleCount + femaleCount;

        DecimalFormat format = new DecimalFormat("###.00");

        List<Option> ageoptions =new ArrayList<>();

        ageoptions.add(new Option("男",Double.parseDouble(format.format(maleCount / sumCount * 100  ))));
        ageoptions.add(new Option("女",Double.parseDouble(format.format( (1 - maleCount / sumCount ) * 100  ))));

        stat.setOptions(ageoptions);
        stat.setTitle("用户性别占比");
        return stat;
    }

    //  将ES中查询的数据，按照指定的格式，封装 gender相关的 stat数据
    @Override
    public JSONObject getESData(String date, Integer startpage, Integer size, String keyword) throws IOException {

        SearchResult searchResult = getDataFromES(date, startpage, size, keyword);

        List<SaleDetail> detailData = getDetailData(searchResult);

        Stat ageStat = getAgeStat(searchResult);

        Stat genderStat = getGenderStat(searchResult);

        JSONObject jsonObject = new JSONObject();

        jsonObject.put("total",searchResult.getTotal());

        ArrayList<Stat> stats = new ArrayList<>();
        stats.add(ageStat);
        stats.add(genderStat);
        jsonObject.put("stat",stats);
        jsonObject.put("detail",detailData);
        return jsonObject;
    }
}
