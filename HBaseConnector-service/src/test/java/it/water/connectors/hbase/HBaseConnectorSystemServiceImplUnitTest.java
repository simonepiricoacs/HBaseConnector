package it.water.connectors.hbase;

import it.water.connectors.hbase.api.options.HBaseConnectorOptions;
import it.water.connectors.hbase.service.HBaseConnectorSystemServiceImpl;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class HBaseConnectorSystemServiceImplUnitTest {

    private HBaseConnectorSystemServiceImpl systemService;

    @Mock
    private Admin admin;

    @Mock
    private Connection connection;

    @Mock
    private HBaseConnectorOptions options;

    @Mock
    private ExecutorService threadPool;

    @Mock
    private Table table;

    @BeforeEach
    void setUp() throws Exception {
        systemService = new HBaseConnectorSystemServiceImpl();
        setField(systemService, "admin", admin);
        setField(systemService, "connection", connection);
        setField(systemService, "hbaseConnectorOptions", options);
        setField(systemService, "hbaseThreadPool", threadPool);
        setField(systemService, "maxScanPageSize", 50);

        lenient().when(options.getAwaitTermination()).thenReturn(1000L);
        lenient().when(connection.getTable(any(TableName.class))).thenReturn(table);
    }

    @Test
    void executeTaskDelegatesToThreadPool() {
        Runnable task = () -> { };
        systemService.executeTask(task);
        verify(threadPool).execute(task);
    }

    @Test
    void deactivateWithGracefulShutdown() throws Exception {
        when(threadPool.awaitTermination(1000L, TimeUnit.MILLISECONDS)).thenReturn(true);

        systemService.deactivate();

        verify(threadPool).shutdown();
        verify(threadPool, never()).shutdownNow();
        verify(admin).close();
        verify(connection).close();
    }

    @Test
    void deactivateWithTimeoutForcesShutdownNow() throws Exception {
        when(threadPool.awaitTermination(1000L, TimeUnit.MILLISECONDS)).thenReturn(false);

        systemService.deactivate();

        verify(threadPool).shutdown();
        verify(threadPool).shutdownNow();
    }

    @Test
    void deactivateOnInterruptedExceptionRethrows() throws Exception {
        when(threadPool.awaitTermination(anyLong(), any(TimeUnit.class))).thenThrow(new InterruptedException("stop"));

        assertThrows(InterruptedException.class, () -> systemService.deactivate());
        verify(threadPool).shutdownNow();
    }

    @Test
    void checkConnectionDelegatesToHBaseAdminAvailable() throws Exception {
        Configuration conf = new Configuration();
        setField(systemService, "configuration", conf);

        try (MockedStatic<HBaseAdmin> mocked = mockStatic(HBaseAdmin.class)) {
            mocked.when(() -> HBaseAdmin.available(conf)).thenAnswer(invocation -> null);

            systemService.checkConnection();

            mocked.verify(() -> HBaseAdmin.available(conf));
        }
    }

    @Test
    void checkConnectionPropagatesIOException() throws Exception {
        Configuration conf = new Configuration();
        setField(systemService, "configuration", conf);

        try (MockedStatic<HBaseAdmin> mocked = mockStatic(HBaseAdmin.class)) {
            mocked.when(() -> HBaseAdmin.available(conf)).thenThrow(new IOException("not-available"));
            assertThrows(IOException.class, () -> systemService.checkConnection());
        }
    }

    @Test
    void createTableAndAsyncDelegate() throws Exception {
        systemService.createTable("table1", Arrays.asList("cf1", "cf2"));
        systemService.createTableAsync("table2", Collections.singletonList("cf1"));

        verify(admin).createTable(any(TableDescriptor.class));
        verify(threadPool).execute(any(Runnable.class));
    }

    @Test
    void deleteDataAndAsyncDelegate() throws Exception {
        systemService.deleteData("table1", "row1");
        systemService.deleteDataAsync("table1", "row2");

        verify(table).delete(any(Delete.class));
        verify(table).close();
        verify(threadPool).execute(any(Runnable.class));
    }

    @Test
    void disableEnableDropAndAsyncDelegate() throws Exception {
        systemService.disableTable("table1");
        systemService.enableTable("table1");
        systemService.dropTable("table1");
        systemService.disableAndDropTable("table1");

        systemService.disableTableAsync("table2");
        systemService.dropTableAsync("table3");
        systemService.disableAndDropTableAsync("table4");

        verify(admin, times(2)).disableTable(TableName.valueOf("table1"));
        verify(admin).enableTable(TableName.valueOf("table1"));
        verify(admin, times(2)).deleteTable(TableName.valueOf("table1"));
        verify(threadPool, times(3)).execute(any(Runnable.class));
    }

    @Test
    void insertDataAndAsyncDelegate() throws Exception {
        systemService.insertData("table1", "row1", "cf1", "col1", "v1");
        systemService.insertDataAsync("table1", "row2", "cf1", "col2", "v2");

        verify(table).put(any(Put.class));
        verify(table).close();
        verify(threadPool).execute(any(Runnable.class));
    }

    @Test
    void tableExistsReturnsTrueFalseAndHandlesErrors() throws Exception {
        when(admin.tableExists(TableName.valueOf("table-yes"))).thenReturn(true);
        when(admin.tableExists(TableName.valueOf("table-no"))).thenReturn(false);
        doThrow(new IOException("error")).when(admin).tableExists(TableName.valueOf("table-error"));

        assertTrue(systemService.tableExists("table-yes"));
        assertFalse(systemService.tableExists("table-no"));
        assertFalse(systemService.tableExists("table-error"));
    }

    @Test
    void getScannerBuildsScanVariants() throws Exception {
        ResultScanner scanner = mock(ResultScanner.class);
        when(table.getScanner(any(Scan.class))).thenReturn(scanner);

        Map<byte[], List<byte[]>> columnsWithQualifier = new HashMap<>();
        columnsWithQualifier.put("cf1".getBytes(), Collections.singletonList("c1".getBytes()));
        Map<byte[], List<byte[]>> columnsFamilyOnly = new HashMap<>();
        columnsFamilyOnly.put("cf2".getBytes(), Collections.emptyList());

        assertNotNull(systemService.getScanner("table1", columnsWithQualifier, null, null, 10));
        assertNotNull(systemService.getScanner("table1", columnsWithQualifier, "a".getBytes(), "z".getBytes(), 100));
        assertNotNull(systemService.getScanner("table1", columnsFamilyOnly, new byte[0], new byte[0], -1));
        assertNotNull(systemService.getScanner("table1", columnsFamilyOnly, null, null, 0));
    }

    @Test
    void scanResultsInvokesConsumerAndHandlesFailures() throws Exception {
        ResultScanner scanner = mock(ResultScanner.class);
        Result first = mock(Result.class);
        Result second = mock(Result.class);
        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.iterator()).thenReturn(Arrays.asList(first, second).iterator());

        Map<byte[], List<byte[]>> columns = new HashMap<>();
        columns.put("cf1".getBytes(), Collections.singletonList("c1".getBytes()));

        AtomicInteger counter = new AtomicInteger(0);
        Consumer<Result> consumer = result -> counter.incrementAndGet();

        systemService.scanResults("table1", columns, null, null, 10, consumer);
        assertEquals(2, counter.get());

        when(connection.getTable(any(TableName.class))).thenThrow(new IOException("connection-error"));
        assertDoesNotThrow(() -> systemService.scanResults("table1", columns, null, null, 10, result -> { }));
    }

    @Test
    void iterateOverResultsInvokesBiConsumerAndHandlesFailures() throws Exception {
        ResultScanner scanner = mock(ResultScanner.class);
        Result first = mock(Result.class);
        Result second = mock(Result.class);
        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.iterator()).thenReturn(Arrays.asList(first, second).iterator());

        Map<byte[], List<byte[]>> columns = new HashMap<>();
        columns.put("cf1".getBytes(), Collections.singletonList("c1".getBytes()));

        List<Boolean> flags = new ArrayList<>();
        BiConsumer<Result, Boolean> consumer = (result, hasMore) -> flags.add(hasMore);

        systemService.iterateOverResults("table1", columns, null, null, 10, consumer);
        assertEquals(Arrays.asList(true, false), flags);

        when(connection.getTable(any(TableName.class))).thenThrow(new IOException("connection-error"));
        assertDoesNotThrow(() -> systemService.iterateOverResults("table1", columns, null, null, 10, (r, h) -> { }));
    }

    @Test
    void scanSingleColumnAndScanWithCompleteResultWork() throws Exception {
        byte[] cf = "cf1".getBytes();
        byte[] col = "c1".getBytes();
        byte[] row = "row1".getBytes();

        Result result = mock(Result.class);
        when(result.getValue(cf, col)).thenReturn("value".getBytes());
        when(result.getRow()).thenReturn(row);

        ResultScanner scanner = mock(ResultScanner.class);
        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.iterator()).thenAnswer(invocation -> Collections.singletonList(result).iterator());

        List<byte[]> list = systemService.scan("table1", cf, col, null, null);
        assertEquals(1, list.size());

        Map<byte[], List<byte[]>> columns = new HashMap<>();
        columns.put(cf, Collections.singletonList(col));

        List<Result> completeResults = systemService.scanWithCompleteResult("table1", columns, null, null, 10);
        assertEquals(1, completeResults.size());

        Map<byte[], Map<byte[], Map<byte[], byte[]>>> mapped = systemService.scan("table1", columns, null, null, 10);
        assertTrue(mapped.containsKey(row));
        assertEquals("value", new String(mapped.get(row).get(cf).get(col)));
    }

    @Test
    void scanMapSkipsNullCellValues() throws Exception {
        byte[] cf = "cf1".getBytes();
        byte[] col = "c1".getBytes();
        byte[] row = "row1".getBytes();

        Result result = mock(Result.class);
        when(result.getValue(cf, col)).thenReturn(null);
        when(result.getRow()).thenReturn(row);

        ResultScanner scanner = mock(ResultScanner.class);
        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.iterator()).thenReturn(Collections.singletonList(result).iterator());

        Map<byte[], List<byte[]>> columns = new HashMap<>();
        columns.put(cf, Collections.singletonList(col));

        Map<byte[], Map<byte[], Map<byte[], byte[]>>> mapped = systemService.scan("table1", columns, null, null, 10);
        assertTrue(mapped.containsKey(row));
        assertTrue(mapped.get(row).get(cf).isEmpty());
    }

    @Test
    void rowCountOverloadsCountCells() throws Throwable {
        Result result = mock(Result.class);
        org.apache.hadoop.hbase.Cell cell = mock(org.apache.hadoop.hbase.Cell.class);
        when(result.listCells()).thenReturn(Arrays.asList(cell, cell));

        ResultScanner scanner = mock(ResultScanner.class);
        when(table.getScanner(any(Scan.class))).thenReturn(scanner);
        when(scanner.iterator()).thenAnswer(invocation -> Collections.singletonList(result).iterator());

        long countByColumn = systemService.rowCount("table1", "cf1".getBytes(), "c1".getBytes(), null, null);
        assertEquals(2, countByColumn);

        Map<byte[], List<byte[]>> columns = new HashMap<>();
        columns.put("cf1".getBytes(), Collections.singletonList("c1".getBytes()));
        long countByMap = systemService.rowCount("table1", columns, null, null);
        assertEquals(2, countByMap);

        when(scanner.iterator()).thenReturn(Collections.emptyIterator());
        long countNoColumn = systemService.rowCount("table1", "cf1".getBytes(), null, null, null);
        assertEquals(0, countNoColumn);
    }

    @Test
    void activateInitializesConfigurationAndClients() throws Exception {
        HBaseConnectorSystemServiceImpl freshService = new HBaseConnectorSystemServiceImpl();
        HBaseConnectorOptions opts = mock(HBaseConnectorOptions.class);

        when(opts.getCorePoolSize()).thenReturn(1);
        when(opts.getMaximumPoolSize()).thenReturn(2);
        when(opts.getKeepAliveTime()).thenReturn(1000L);
        when(opts.getMaxScanPageSize()).thenReturn(100);
        when(opts.getClusterDistributed()).thenReturn(true);
        when(opts.getMasterHostname()).thenReturn("master");
        when(opts.getMasterPort()).thenReturn(26000);
        when(opts.getMasterInfoPort()).thenReturn(26010);
        when(opts.getRegionserverPort()).thenReturn(26020);
        when(opts.getRegionserverInfoPort()).thenReturn(26030);
        when(opts.getRootdir()).thenReturn("hdfs://cluster/hbase");
        when(opts.getZookeeperQuorum()).thenReturn("zk-1,zk-2");
        when(opts.getAwaitTermination()).thenReturn(1000L);

        setField(freshService, "hbaseConnectorOptions", opts);

        try (MockedStatic<HBaseConfiguration> hbaseConfiguration = mockStatic(HBaseConfiguration.class);
             MockedStatic<ConnectionFactory> connectionFactory = mockStatic(ConnectionFactory.class)) {
            Configuration conf = new Configuration();
            hbaseConfiguration.when(HBaseConfiguration::create).thenReturn(conf);

            Connection mockedConnection = mock(Connection.class);
            Admin mockedAdmin = mock(Admin.class);
            connectionFactory.when(() -> ConnectionFactory.createConnection(conf)).thenReturn(mockedConnection);
            when(mockedConnection.getAdmin()).thenReturn(mockedAdmin);

            freshService.activate();
            awaitCondition(() -> {
                try {
                    Object value = getField(freshService, "maxScanPageSize");
                    return value instanceof Integer && ((Integer) value) == 100;
                } catch (Exception e) {
                    return false;
                }
            }, 5, TimeUnit.SECONDS);

            assertNotNull(getField(freshService, "configuration"));
            assertNotNull(getField(freshService, "hbaseThreadPool"));
            assertEquals(100, getField(freshService, "maxScanPageSize"));

            freshService.deactivate();
        }
    }

    @Test
    void activateHandlesConnectionFailureWithoutThrowing() throws Exception {
        HBaseConnectorSystemServiceImpl freshService = new HBaseConnectorSystemServiceImpl();
        HBaseConnectorOptions opts = mock(HBaseConnectorOptions.class);

        when(opts.getCorePoolSize()).thenReturn(1);
        when(opts.getMaximumPoolSize()).thenReturn(2);
        when(opts.getKeepAliveTime()).thenReturn(1000L);
        when(opts.getMaxScanPageSize()).thenReturn(100);
        when(opts.getClusterDistributed()).thenReturn(false);
        when(opts.getMasterHostname()).thenReturn("master");
        when(opts.getMasterPort()).thenReturn(26000);
        when(opts.getMasterInfoPort()).thenReturn(26010);
        when(opts.getRegionserverPort()).thenReturn(26020);
        when(opts.getRegionserverInfoPort()).thenReturn(26030);
        when(opts.getRootdir()).thenReturn("hdfs://cluster/hbase");
        when(opts.getZookeeperQuorum()).thenReturn("zk");
        when(opts.getAwaitTermination()).thenReturn(1000L);

        setField(freshService, "hbaseConnectorOptions", opts);

        try (MockedStatic<HBaseConfiguration> hbaseConfiguration = mockStatic(HBaseConfiguration.class);
             MockedStatic<ConnectionFactory> connectionFactory = mockStatic(ConnectionFactory.class)) {
            Configuration conf = new Configuration();
            hbaseConfiguration.when(HBaseConfiguration::create).thenReturn(conf);
            connectionFactory.when(() -> ConnectionFactory.createConnection(conf)).thenThrow(new IOException("connection-refused"));

            assertDoesNotThrow(freshService::activate);
            awaitCondition(() -> {
                try {
                    return getField(freshService, "configuration") != null;
                } catch (Exception e) {
                    return false;
                }
            }, 2, TimeUnit.SECONDS);

            Object pool = getField(freshService, "hbaseThreadPool");
            assertNotNull(pool);
            ((ExecutorService) pool).shutdownNow();
        }
    }

    @Test
    void wrapperMethodsSwallowIoExceptions() throws Exception {
        doThrow(new IOException("create-error")).when(admin).createTable(any(org.apache.hadoop.hbase.HTableDescriptor.class));
        doThrow(new IOException("disable-error")).when(admin).disableTable(any(TableName.class));
        doThrow(new IOException("enable-error")).when(admin).enableTable(any(TableName.class));
        doThrow(new IOException("drop-error")).when(admin).deleteTable(any(TableName.class));
        when(connection.getTable(any(TableName.class))).thenThrow(new IOException("table-error"));

        assertDoesNotThrow(() -> systemService.createTable("table1", Collections.singletonList("cf1")));
        assertDoesNotThrow(() -> systemService.deleteData("table1", "row1"));
        assertDoesNotThrow(() -> systemService.disableTable("table1"));
        assertDoesNotThrow(() -> systemService.enableTable("table1"));
        assertDoesNotThrow(() -> systemService.dropTable("table1"));
        assertDoesNotThrow(() -> systemService.disableAndDropTable("table1"));
        assertDoesNotThrow(() -> systemService.insertData("table1", "row1", "cf1", "c1", "v1"));
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                field.set(target, value);
                return;
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    private static Object getField(Object target, String fieldName) throws Exception {
        Class<?> clazz = target.getClass();
        while (clazz != null) {
            try {
                Field field = clazz.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(target);
            } catch (NoSuchFieldException e) {
                clazz = clazz.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }

    private static void awaitCondition(BooleanSupplier condition, long timeout, TimeUnit unit) throws InterruptedException {
        long deadline = System.nanoTime() + unit.toNanos(timeout);
        while (System.nanoTime() < deadline) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.yield();
        }
        fail("Condition not met within timeout");
    }
}
