package it.water.connectors.hbase;

import it.water.connectors.hbase.api.HBaseConnectorApi;
import it.water.connectors.hbase.service.rest.HBaseConnectorRestControllerImpl;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import javax.ws.rs.core.Response;
import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class HBaseConnectorRestControllerImplUnitTest {

    @Mock
    private HBaseConnectorApi hbaseConnectorApi;

    @InjectMocks
    private HBaseConnectorRestControllerImpl controller;

    @Test
    void checkConnectionReturnsOkWhenServiceWorks() throws IOException {
        String result = controller.checkConnection();

        assertEquals("Connection OK", result);
        verify(hbaseConnectorApi).checkConnection();
    }

    @Test
    void checkConnectionThrowsWhenServiceFails() throws IOException {
        doThrow(new IOException("connection down")).when(hbaseConnectorApi).checkConnection();

        assertThrows(IOException.class, () -> controller.checkConnection());
        verify(hbaseConnectorApi).checkConnection();
    }

    @Test
    void checkModuleWorkingReturnsStaticOkMessage() {
        Response result = controller.checkModuleWorking();

        assertEquals(200, result.getStatus());
        assertEquals("HBaseConnector Module works!", result.getEntity());
    }

    @Test
    void tableOperationsDelegateToApiWithoutConnectorLookup() throws IOException {
        controller.createTable("tbl", "cf1,cf2");
        controller.insertData("tbl", "row1", "cf1", "col1", "v1");
        controller.deleteData("tbl", "row1");
        controller.enableTable("tbl");
        controller.disableTable("tbl");
        controller.dropTable("tbl");

        verify(hbaseConnectorApi).createTable("tbl", java.util.Arrays.asList("cf1", "cf2"));
        verify(hbaseConnectorApi).insertData("tbl", "row1", "cf1", "col1", "v1");
        verify(hbaseConnectorApi).deleteData("tbl", "row1");
        verify(hbaseConnectorApi).enableTable("tbl");
        verify(hbaseConnectorApi).disableTable("tbl");
        verify(hbaseConnectorApi).dropTable("tbl");
    }

    @Test
    void createTableHandlesSingleFamilyValue() throws IOException {
        controller.createTable("tbl", "cf1");

        verify(hbaseConnectorApi, times(1)).createTable("tbl", java.util.Arrays.asList("cf1"));
    }
}
