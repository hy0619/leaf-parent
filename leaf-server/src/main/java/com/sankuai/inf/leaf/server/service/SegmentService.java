package com.sankuai.inf.leaf.server.service;

import cn.hutool.core.util.RandomUtil;
import cn.hutool.http.HttpUtil;
import com.alibaba.druid.pool.DruidDataSource;
import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.PropertyFactory;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.ZeroIDGen;
import com.sankuai.inf.leaf.segment.SegmentIDGenImpl;
import com.sankuai.inf.leaf.segment.dao.IDAllocDao;
import com.sankuai.inf.leaf.segment.dao.impl.IDAllocDaoImpl;
import com.sankuai.inf.leaf.server.Constants;
import com.sankuai.inf.leaf.server.exception.InitException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;

@Service("SegmentService")
public class SegmentService {
    private Logger logger = LoggerFactory.getLogger(SegmentService.class);

    private IDGen idGen;
    private DruidDataSource dataSource;

    public SegmentService() throws SQLException, InitException {
        Properties properties = PropertyFactory.getProperties();
        boolean flag = Boolean.parseBoolean(properties.getProperty(Constants.LEAF_SEGMENT_ENABLE, "true"));
        if (flag) {
            // Config dataSource
            dataSource = new DruidDataSource();
            dataSource.setUrl(properties.getProperty(Constants.LEAF_JDBC_URL));
            dataSource.setUsername(properties.getProperty(Constants.LEAF_JDBC_USERNAME));
            dataSource.setPassword(properties.getProperty(Constants.LEAF_JDBC_PASSWORD));
            dataSource.setDriverClassName(properties.getProperty(Constants.LEAF_DRIVER_CLASS_NAME));
            dataSource.init();

            // Config Dao
            IDAllocDao dao = new IDAllocDaoImpl(dataSource);

            // Config ID Gen
            idGen = new SegmentIDGenImpl();
            ((SegmentIDGenImpl) idGen).setDao(dao);
            if (idGen.init()) {
                logger.info("Segment Service Init Successfully");
            } else {
                throw new InitException("Segment Service Init Fail");
            }
        } else {
            idGen = new ZeroIDGen();
            logger.info("Zero ID Gen Service Init Successfully");
        }
    }

    public Result getId(String key) {
        return idGen.get(key);
    }

    public SegmentIDGenImpl getIdGen() {
        if (idGen instanceof SegmentIDGenImpl) {
            return (SegmentIDGenImpl) idGen;
        }
        return null;
    }


    public static void main(String[] args) throws InterruptedException {

        int forCnt = 2;

        int forThreadCnt = 300;

        final List<String> urlList = new ArrayList();

        final Set<Long> rightIdSet = Collections.synchronizedSet(new HashSet<Long>());


        final CountDownLatch countDownLatch = new CountDownLatch( forCnt * forThreadCnt);

        urlList.add("http://localhost:8001/api/segment/get/leaf-segment-test");
        urlList.add("http://localhost:8002/api/segment/get/leaf-segment-test");
        /*urlList.add("http://localhost:8003/api/segment/get/leaf-segment-test");
        urlList.add("http://localhost:8004/api/segment/get/leaf-segment-test");
        urlList.add("http://localhost:8005/api/segment/get/leaf-segment-test");*/


        for(int i = 0 ; i < forCnt ; i++){
            final int rNum = RandomUtil.randomInt(0,urlList.size());

            for(int j = 0 ; j<forThreadCnt ; j++){
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try{
                            String rStr = HttpUtil.get(urlList.get(rNum));


                            System.out.println("id===>" + rStr);
                                rightIdSet.add(Long.parseLong(rStr));

                        }finally {
                            countDownLatch.countDown();
                        }
                    }
                }).start();
            }

        }

        countDownLatch.await();

        System.out.println("===rightIdSet===" + rightIdSet.size());

    }
}
