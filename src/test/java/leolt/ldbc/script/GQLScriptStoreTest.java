package leolt.ldbc.script;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class GQLScriptStoreTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(GQLScriptStoreTest.class);

    @Test (enabled = false)
    public void testGQLScriptStore() throws IOException {
        Assert.assertTrue(GQLScriptStore.getBi01Gql().startsWith("--博文摘要"));
        Assert.assertTrue(GQLScriptStore.getBi02Gql().startsWith("--标签演变"));
        Assert.assertTrue(GQLScriptStore.getBi03Gql().startsWith("--一个国家的热门话题"));
        Assert.assertTrue(GQLScriptStore.getBi04Gql().startsWith("--按国家/地区划分的主要消息创建者"));
        Assert.assertTrue(GQLScriptStore.getBi05Gql().startsWith("--给定主题的最活跃博主"));
        Assert.assertTrue(GQLScriptStore.getBi06Gql().startsWith("--给定主题的最权威用户"));
        Assert.assertTrue(GQLScriptStore.getBi07Gql().startsWith("--相关话题"));
        Assert.assertTrue(GQLScriptStore.getBi08Gql().startsWith("--标签的中心人物"));
        Assert.assertTrue(GQLScriptStore.getBi09Gql().startsWith("--顶级话题线发起者"));
        Assert.assertTrue(GQLScriptStore.getBi10Gql().startsWith("--社交圈专家"));

        Assert.assertTrue(GQLScriptStore.getBi11Gql().startsWith("--朋友三角"));
        Assert.assertTrue(GQLScriptStore.getBi12Gql().startsWith("--有多少人有给定数量的消息"));
        Assert.assertTrue(GQLScriptStore.getBi13Gql().startsWith("--一个国家的僵尸数"));
        Assert.assertTrue(GQLScriptStore.getBi14Gql().startsWith("--国际对话"));
        Assert.assertTrue(GQLScriptStore.getBi15Gql().startsWith("--给定时间范围内创建的论坛可信连接路径"));
        Assert.assertTrue(GQLScriptStore.getBi16Gql().startsWith("--假新闻检测"));
        Assert.assertTrue(GQLScriptStore.getBi17Gql().startsWith("--信息传播分析"));
        Assert.assertTrue(GQLScriptStore.getBi18Gql().startsWith("--朋友推荐"));
        Assert.assertTrue(GQLScriptStore.getBi19Gql().startsWith("--城市间的互动路径"));
        Assert.assertTrue(GQLScriptStore.getBi20Gql().startsWith("--招聘"));
    }
}
