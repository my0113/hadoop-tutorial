package cn.itcast.hadoop.yarn.shuffle;

import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;

import java.nio.ByteBuffer;

/**
 * 自定义ShuffleHandler
 *      在yarn-site.xml中，默认MapReduce的shuffle实现是ShuffleHandler（yarn.nodemanager.aux-services.mapreduce_shuffle.class=org.apache.hadoop.mapred.ShuffleHandler）
 *      考虑到为了支持更多的
 *
 *
 *
 *
 * @ClassName MyShuffleHandler
 * @Description
 * @Created by MengYao
 * @Date 2020/10/13 18:05
 * @Version V1.0
 */
public class MyShuffleHandler extends AuxiliaryService {

    protected MyShuffleHandler(String name) {
        super(name);
    }

    @Override
    public void initializeApplication(ApplicationInitializationContext initAppContext) {

    }

    @Override
    public void stopApplication(ApplicationTerminationContext stopAppContext) {

    }

    @Override
    public ByteBuffer getMetaData() {
        return null;
    }
}
