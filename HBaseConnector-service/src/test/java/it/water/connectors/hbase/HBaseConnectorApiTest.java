package it.water.connectors.hbase;

import it.water.connectors.hbase.api.HBaseConnectorApi;
import it.water.connectors.hbase.api.HBaseConnectorSystemApi;
import it.water.connectors.hbase.api.options.HBaseConnectorOptions;
import it.water.core.api.registry.ComponentRegistry;
import it.water.core.api.service.Service;
import it.water.core.interceptors.annotations.Inject;
import it.water.core.testing.utils.junit.WaterTestExtension;
import lombok.Setter;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Generated with Water Generator.
 * Test class for HBaseConnector service-only module.
 */
@ExtendWith(WaterTestExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class HBaseConnectorApiTest implements Service {

    @Inject
    @Setter
    private ComponentRegistry componentRegistry;

    @Inject
    @Setter
    private HBaseConnectorApi hBaseConnectorApi;

    @Inject
    @Setter
    private HBaseConnectorSystemApi hBaseConnectorSystemApi;

    @Inject
    @Setter
    private HBaseConnectorOptions hBaseConnectorOptions;

    @Test
    @Order(1)
    void componentsInstantiatedCorrectly() {
        this.hBaseConnectorApi = this.componentRegistry.findComponent(HBaseConnectorApi.class, null);
        this.hBaseConnectorSystemApi = this.componentRegistry.findComponent(HBaseConnectorSystemApi.class, null);
        this.hBaseConnectorOptions = this.componentRegistry.findComponent(HBaseConnectorOptions.class, null);

        assertNotNull(this.hBaseConnectorApi);
        assertNotNull(this.hBaseConnectorSystemApi);
        assertNotNull(this.hBaseConnectorOptions);
    }

    @Test
    @Order(2)
    void optionsExposeValidDefaults() {
        assertNotNull(hBaseConnectorOptions.getMasterHostname());
        assertNotNull(hBaseConnectorOptions.getZookeeperQuorum());
        assertNotNull(hBaseConnectorOptions.getRootdir());
        assertTrue(hBaseConnectorOptions.getMasterPort() > 0);
        assertTrue(hBaseConnectorOptions.getMasterInfoPort() > 0);
        assertTrue(hBaseConnectorOptions.getRegionserverPort() > 0);
        assertTrue(hBaseConnectorOptions.getRegionserverInfoPort() > 0);
        assertTrue(hBaseConnectorOptions.getCorePoolSize() > 0);
        assertTrue(hBaseConnectorOptions.getMaximumPoolSize() >= hBaseConnectorOptions.getCorePoolSize());
        assertTrue(hBaseConnectorOptions.getAwaitTermination() >= 0);
        assertTrue(hBaseConnectorOptions.getMaxScanPageSize() > 0);
    }

    @Test
    @Order(3)
    void fireAndForgetOperationsDoNotThrow() {
        assertDoesNotThrow(() -> hBaseConnectorApi.createTable("unit-table", Collections.singletonList("cf1")));
        assertDoesNotThrow(() -> hBaseConnectorApi.insertData("unit-table", "row-1", "cf1", "c1", "v1"));
        assertDoesNotThrow(() -> hBaseConnectorApi.deleteData("unit-table", "row-1"));
        assertDoesNotThrow(() -> hBaseConnectorApi.disableTable("unit-table"));
        assertDoesNotThrow(() -> hBaseConnectorApi.enableTable("unit-table"));
        assertDoesNotThrow(() -> hBaseConnectorApi.dropTable("unit-table"));
    }
}
