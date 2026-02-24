package it.water.connectors.hbase;

import it.water.connectors.hbase.service.options.HBaseConnectorOptionsImpl;
import it.water.core.api.bundle.ApplicationProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HBaseConnectorOptionsImplUnitTest {

    @Mock
    private ApplicationProperties applicationProperties;

    private HBaseConnectorOptionsImpl options;

    @BeforeEach
    void setUp() {
        options = new HBaseConnectorOptionsImpl();
        options.setApplicationProperties(applicationProperties);
    }

    @Test
    void returnsConfiguredValues() {
        when(applicationProperties.getPropertyOrDefault("hbaseconnector.master.hostname", "localhost")).thenReturn("master.local");
        when(applicationProperties.getPropertyOrDefault("hbaseconnector.master.port", "16000")).thenReturn("26000");
        when(applicationProperties.getPropertyOrDefault("hbaseconnector.zookeeper.quorum", "localhost")).thenReturn("zk-1,zk-2");
        when(applicationProperties.getPropertyOrDefault("hbaseconnector.rootdir", "hdfs://localhost:8020/hbase")).thenReturn("hdfs://cluster/hbase");
        when(applicationProperties.getPropertyOrDefault("hbaseconnector.cluster.distributed", "false")).thenReturn("true");
        when(applicationProperties.getPropertyOrDefault("hbaseconnector.master.info.port", "16010")).thenReturn("26010");
        when(applicationProperties.getPropertyOrDefault("hbaseconnector.regionserver.port", "16020")).thenReturn("26020");
        when(applicationProperties.getPropertyOrDefault("hbaseconnector.regionserver.info.port", "16030")).thenReturn("26030");
        when(applicationProperties.getPropertyOrDefault("hbaseconnector.thread.corepool.size", "10")).thenReturn("4");
        when(applicationProperties.getPropertyOrDefault("hbaseconnector.thread.maximum.size", "20")).thenReturn("8");
        when(applicationProperties.getPropertyOrDefault("hbaseconnector.thread.keepalive.time", "0")).thenReturn("1500");
        when(applicationProperties.getPropertyOrDefault("hbaseconnector.thread.await.termination", "1000")).thenReturn("2500");
        when(applicationProperties.getPropertyOrDefault("hbaseconnector.scan.max.page.size", "2147483647")).thenReturn("500");

        assertEquals("master.local", options.getMasterHostname());
        assertEquals(26000, options.getMasterPort());
        assertEquals("zk-1,zk-2", options.getZookeeperQuorum());
        assertEquals("hdfs://cluster/hbase", options.getRootdir());
        assertTrue(options.getClusterDistributed());
        assertEquals(26010, options.getMasterInfoPort());
        assertEquals(26020, options.getRegionserverPort());
        assertEquals(26030, options.getRegionserverInfoPort());
        assertEquals(4, options.getCorePoolSize());
        assertEquals(8, options.getMaximumPoolSize());
        assertEquals(1500L, options.getKeepAliveTime());
        assertEquals(2500L, options.getAwaitTermination());
        assertEquals(500, options.getMaxScanPageSize());
    }

    @Test
    void returnsDefaultValuesWhenMissing() {
        when(applicationProperties.getPropertyOrDefault(anyString(), anyString())).thenAnswer(invocation -> invocation.getArgument(1, String.class));

        assertEquals("localhost", options.getMasterHostname());
        assertEquals(16000, options.getMasterPort());
        assertEquals("localhost", options.getZookeeperQuorum());
        assertEquals("hdfs://localhost:8020/hbase", options.getRootdir());
        assertFalse(options.getClusterDistributed());
        assertEquals(16010, options.getMasterInfoPort());
        assertEquals(16020, options.getRegionserverPort());
        assertEquals(16030, options.getRegionserverInfoPort());
        assertEquals(10, options.getCorePoolSize());
        assertEquals(20, options.getMaximumPoolSize());
        assertEquals(0L, options.getKeepAliveTime());
        assertEquals(1000L, options.getAwaitTermination());
        assertEquals(2147483647, options.getMaxScanPageSize());
    }
}
