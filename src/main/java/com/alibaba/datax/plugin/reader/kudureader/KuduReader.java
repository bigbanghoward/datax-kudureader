package com.alibaba.datax.plugin.reader.kudureader;

import com.alibaba.datax.common.element.*;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.plugin.TaskPluginCollector;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.*;
import org.apache.kudu.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.*;

public class KuduReader extends Reader {
    private static Logger LOG = LoggerFactory.getLogger(KuduReader.class);

    public static class Job extends Reader.Job {
        private static final Logger LOG = LoggerFactory.getLogger(Job.class);
        private Configuration originalDataxConfig = null;
        private String kerberosPrincipal;
        private String kerberosKeytabFilePath;
        private boolean haveKerberos;

        @Override
        public void init() {
            LOG.info("datax-kudureader init..");
            this.originalDataxConfig = super.getPluginJobConf();
            validateConfig();
        }

        private void validateConfig() {
            LOG.debug("datax-kudueader originalConfig json is:[\n{}\n]", originalDataxConfig.toJSON());

            this.haveKerberos = originalDataxConfig.getBool("ENABLE_KERBEROS", false);
            this.kerberosKeytabFilePath = originalDataxConfig.getString("KERBEROS_KEYTAB_FILE_PATH", null);
            this.kerberosPrincipal = originalDataxConfig.getString("KERBEROS_PRINCIPAL", null);
            LOG.info("kerberosKeytabFilePath: {}, kerberosPrincipal: {}", kerberosKeytabFilePath, kerberosPrincipal);

            if (haveKerberos) {
                if (StringUtils.isNotBlank(this.kerberosPrincipal) && Paths.get(this.kerberosKeytabFilePath).toFile().exists()) {
                    this.kerberosAuthentication(this.kerberosPrincipal, this.kerberosKeytabFilePath);
                    LOG.info("************ kerberosKeytabFilePath: {}, kerberosPrincipal: {} *************", kerberosKeytabFilePath, kerberosPrincipal);
                } else {
                    LOG.error("kerberosKeytabFilePath: {}, kerberosPrincipal: {}", kerberosKeytabFilePath, kerberosPrincipal);
                }
            }
        }

        private void kerberosAuthentication(String kerberosPrincipal, String kerberosKeytabFilePath) {
            System.setProperty("java.security.krb5.conf", "/etc/krb5.conf");
            if (StringUtils.isNotBlank(this.kerberosPrincipal) && StringUtils.isNotBlank(this.kerberosKeytabFilePath)) {
                UserGroupInformation.setConfiguration(new org.apache.hadoop.conf.Configuration());
                try {
                    UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
                } catch (Exception e) {
                    String message = String.format("kerberos认证失败,kerberosKeytabFilePath[%s]和kerberosPrincipal[%s]", kerberosKeytabFilePath, kerberosPrincipal);
                    LOG.error("kerberos认证失败", message);
                    throw DataXException.asDataXException(KuduReaderErrorCode.KUDU_PARAM_NECESSARY, KuduConstant.KERBEROS);
                }
            }
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            // 一般来说，是需要推迟到 task 中进行pre 的执行（单表情况例外）
            LOG.info("kudu no need to split ");
            return Lists.newArrayList(originalDataxConfig);
        }


        @Override
        public void post() {
            // 一般来说，是需要推迟到 task 中进行post 的执行（单表情况例外）
            LOG.info("kudu reader post...");
        }

        @Override
        public void destroy() {
            LOG.info("kudu reader destroy...");
        }

    }

    public static class Task extends Reader.Task {
        private Configuration readerSliceConfig;
        private Logger LOG = LoggerFactory.getLogger(Task.class);
        KuduHelper kuduHelper;

        @Override
        public void init() {
            readerSliceConfig = super.getPluginJobConf();
            kuduHelper = new KuduHelper(readerSliceConfig);
            LOG.info("datax-reader task init...");
        }

        @Override
        public void destroy() {
            if (Objects.nonNull(kuduHelper)) {
                kuduHelper.close();
            }
            LOG.info("datax-reader task destroy...");
        }

        @Override
        public void startRead(RecordSender recordSender) {
            LOG.info("datax-reader task startRead...");
            TaskPluginCollector taskPluginCollector = super.getTaskPluginCollector();
            kuduHelper.doReader(recordSender, taskPluginCollector);
        }
    }

    public static class KuduHelper {
        private Logger LOG = LoggerFactory.getLogger(KuduHelper.class);
        //从json里获取的kudu的配置
        private Configuration dataxTaskConfiguration;
        private KuduClient kuduClient;
        private KuduTable kuduTable;
        private KuduSession kuduSession;
        private String kuduMaster;
        private KuduScanner.KuduScannerBuilder kuduScannerBuilder;
        //获取的表的schema
        private Schema kuduTableSchema;
        private Configuration kuduTableWhere;
        private String kuduTableName;
        private List<String> kuduTableColumnNames;
        //需要kudu-reader额外输出的扩展参数，在datax配置文件中配置传入
        private Map<String, String> outputExtendParam;

        int kuduBatchSizeBytes = 1024 * 1024 * 10;  //10M

        private final static String COLUMNS_ALL = "*";

        //构造KuduHelper
        public KuduHelper(Configuration configuration) {
            this.dataxTaskConfiguration = configuration;

            //参数检查
            checkKuduParam();

            //初始化连接
            initKuduClient();
        }

        /**
         * 检查必须的参数
         * kuduMaster
         * connection
         * table
         * column
         */
        public void checkKuduParam() {
            String kudureader_conf_json = this.dataxTaskConfiguration.toJSON();
            LOG.info("kudureader_conf_json:{}", kudureader_conf_json);

            //检查columns参数
            this.kuduTableColumnNames = this.dataxTaskConfiguration.getList(KuduConstant.COLUMNS, String.class);
            if (Objects.isNull(kuduTableColumnNames) || kuduTableColumnNames.size() == 0) {
                throw DataXException.asDataXException(KuduReaderErrorCode.KUDU_PARAM_NECESSARY, String.format("kudureader缺少columns参数."));
            }

            //额外通过kudureader进行输出的参数map
            Configuration extendParamConf = this.dataxTaskConfiguration.getConfiguration(KuduConstant.EXTEND_PARAM);
            if (Objects.nonNull(extendParamConf)) {
                this.outputExtendParam = new HashMap<>();
                Set<String> keys = extendParamConf.getKeys();
                if (Objects.nonNull(keys)) {
                    for (String key : keys) {
                        if (Objects.nonNull(key)) {
                            String value = extendParamConf.getString(key);
                            if (Objects.nonNull(value)) {
                                this.outputExtendParam.put(key, value);
                            }
                        }
                    }
                }
            }

            Configuration configuration = this.dataxTaskConfiguration.getConfiguration(KuduConstant.CONNECTION);
            this.kuduMaster = configuration.getNecessaryValue(KuduConstant.KUDU_MASTER, KuduReaderErrorCode.KUDU_PARAM_NECESSARY);
            this.kuduTableName = configuration.getNecessaryValue(KuduConstant.TABLE, KuduReaderErrorCode.KUDU_PARAM_NECESSARY);
            this.kuduBatchSizeBytes = configuration.getInt(KuduConstant.KUDU_BATCH_SIZE_BYTES, kuduBatchSizeBytes);

            this.kuduTableWhere = this.dataxTaskConfiguration.getConfiguration(KuduConstant.WHERE);
            LOG.info("configuration kuduMaster={}, kuduTableName={},kuduTableColumnNames ={},kuduTableWhere ={}", kuduMaster, kuduTableName, kuduTableColumnNames, kuduTableWhere);
        }


        private String getKuduTableName(KuduClient kuduClient, String tableName) throws KuduException {
            //为了支持impala管理的kudu表，对传入的表名进行处理
            if (!kuduClient.tableExists(tableName)) {
                //表名增加impala前缀
                String impalaTableName = KuduConstant.IMPALA_PREFIX + tableName;
                KuduReader.LOG.info("getKuduTableName tableName:[{}] -> impalaTableName:[{}]", tableName, impalaTableName);
                return impalaTableName;
            } else {
                return tableName;
            }
        }

        //对开始创建各种连接
        public void initKuduClient() {
            LOG.info("initKuduClient...");

            try {
                kuduClient = new KuduClient.KuduClientBuilder(kuduMaster).build();
                kuduSession = kuduClient.newSession();

                HostAndPort leaderMasterServer = kuduClient.findLeaderMasterServer();
                String host = leaderMasterServer.getHost();
                int port = leaderMasterServer.getPort();
                KuduReader.LOG.info("kudu masterServer host:[{}],port:[{}]", host, port);

                this.kuduTableName = getKuduTableName(kuduClient, kuduTableName);
                if (!kuduClient.tableExists(kuduTableName)) {
                    throw new RuntimeException("kudu table is not exists.tableName:" + kuduTableName);
                }

                kuduTable = kuduClient.openTable(kuduTableName);
                kuduTableSchema = kuduTable.getSchema();
                kuduScannerBuilder = kuduClient.newScannerBuilder(kuduTable);
                kuduScannerBuilder.batchSizeBytes(kuduBatchSizeBytes);
                kuduTableColumnNames = getColumnMeta(kuduTableSchema);
            } catch (KuduException e) {
                e.printStackTrace();
                LOG.error("", e);
            }
        }

        public void doReader(RecordSender dataxRecordSender, TaskPluginCollector taskPluginCollector) {
            //处理where
            if (kuduTableWhere != null) {
                handleWhere(kuduScannerBuilder);
            }

            LOG.info("kudu table columns={}", kuduTableColumnNames);

            KuduScanner scanner = kuduScannerBuilder
                    .setProjectedColumnNames(kuduTableColumnNames)
                    .build();

            try {
                long counter = 0L;
                while (scanner.hasMoreRows()) {
                    RowResultIterator rowResults = scanner.nextRows();

                    while (rowResults.hasNext()) {
                        //output record
                        RowResult rowRes = rowResults.next();
                        Record outputRecord = dataxRecordSender.createRecord();

                        //简单封装成json字符串输出
                        //也可以自己封装Record输出
                        JSONObject jsonObject = new JSONObject();
                        for (String columnName : kuduTableColumnNames) {
                            if (rowRes.isNull(columnName)) {
                                jsonObject.put(columnName, "");
                            } else {
                                Object object = rowRes.getObject(columnName);
                                jsonObject.put(columnName, object);
                            }
                        }

                        fillExtendParam(jsonObject);

                        String jsonString = jsonObject.toJSONString();
                        StringColumn stringColumn = new StringColumn(jsonString);
                        outputRecord.addColumn(stringColumn);
                        dataxRecordSender.sendToWriter(outputRecord);

                        counter++;
                    }
                }

                LOG.info("kudu-reader scanner close.reader counter:{}", counter);
            } catch (KuduException e) {
                LOG.error("kudu read error ,", e);
                e.printStackTrace();
            } finally {
                if (Objects.nonNull(scanner)) {
                    try {
                        scanner.close();
                    } catch (KuduException e) {
                        LOG.error("", e);
                        throw new RuntimeException(e);
                    }
                }
            }
        }

        private void fillExtendParam(JSONObject jsonObject) {
            if (Objects.nonNull(jsonObject) && Objects.nonNull(this.outputExtendParam)) {
                this.outputExtendParam.entrySet().stream().forEach(entry -> {
                    String key = entry.getKey();
                    String value = entry.getValue();

                    jsonObject.put(key, value);
                });
            }
        }

        private List<String> getColumnMeta(Schema kuduTableSchema) {
            List<String> columnNames = Lists.newArrayList();

            boolean all_columns = COLUMNS_ALL.equals(this.kuduTableColumnNames.get(0)) ? true : false;

            //处理 column =*
            List<ColumnSchema> tableColumns = kuduTableSchema.getColumns();
            for (ColumnSchema column : tableColumns) {
                String columnName = column.getName();
                Type type = column.getType();
                String typeName = type.getName();

                if (!all_columns && !this.kuduTableColumnNames.contains(columnName)) {
                    continue;
                }

                columnNames.add(columnName);
                LOG.info("Kudu tableName:{},column-name:{},column-type:{}", kuduTableName, columnName, typeName);
            }

            return columnNames;
        }

        public void close() {
            if (kuduSession != null) {
                try {
                    kuduSession.close();
                    LOG.info("session close...");
                } catch (KuduException e) {
                    LOG.error("", e);
                    e.printStackTrace();
                }
            }

            if (kuduClient != null) {
                try {
                    kuduClient.close();
                    LOG.info("kuduClient close...");
                } catch (KuduException e) {
                    LOG.error("", e);
                    e.printStackTrace();
                }
            }
        }

        /**
         * 这里可以需要进行kuduTable的where条件过滤
         * 目前简单实现了单个where条件的处理
         * 可以自己扩展
         * kudureader config:
         * "where": {
         * "column": "p_date",
         * "compare": "=",
         * "value": "2021-06-01"
         * }
         *
         * @param kuduScannerBuilder
         */
        public void handleWhere(KuduScanner.KuduScannerBuilder kuduScannerBuilder) {
            KuduReader.LOG.info("handleWhere where:{}", kuduTableWhere);

            String column = kuduTableWhere.getNecessaryValue(KuduConstant.COLUMN, KuduReaderErrorCode.KUDU_PARAM_NECESSARY).trim();
            String compare = kuduTableWhere.getNecessaryValue(KuduConstant.COMPARE, KuduReaderErrorCode.KUDU_PARAM_NECESSARY).trim();
            String value = kuduTableWhere.getNecessaryValue(KuduConstant.VALUE, KuduReaderErrorCode.KUDU_PARAM_NECESSARY).trim();

            ColumnSchema schemaColumn = kuduTableSchema.getColumn(column);
            KuduPredicate.ComparisonOp comparisonOp;

            switch (compare) {
                case ">":
                    comparisonOp = KuduPredicate.ComparisonOp.GREATER;
                    break;
                case ">=":
                    comparisonOp = KuduPredicate.ComparisonOp.GREATER_EQUAL;
                    break;
                case "<":
                    comparisonOp = KuduPredicate.ComparisonOp.LESS;
                    break;
                case "<=":
                    comparisonOp = KuduPredicate.ComparisonOp.LESS_EQUAL;
                    break;
                case "=":
                    comparisonOp = KuduPredicate.ComparisonOp.EQUAL;
                    break;
                default:
                    throw new IllegalStateException("Unexpected where compare:" + compare);
            }

            Object row;
            switch (schemaColumn.getType()) {
                case INT8:
                    row = Byte.parseByte(value);
                    break;
                case INT16:
                    row = Short.parseShort(value);
                    break;
                case INT32:
                case INT64:
                    row = Integer.parseInt(value);
                    break;
                case UNIXTIME_MICROS:
                    row = Long.parseLong(value);
                    break;
                case STRING:
                    row = value;
                    break;
                case DECIMAL:
                    row = new BigDecimal(value);
                    break;
                case BOOL:
                    row = Boolean.valueOf(value);
                    break;
                case DOUBLE:
                    row = Double.valueOf(value);
                    break;
                case FLOAT:
                    row = Float.valueOf(value);
                    break;
                default:
                    LOG.error("kudu's column:'{}' is unknown type -- ignoring the column.", schemaColumn);
                    throw new DataXException(KuduReaderErrorCode.KUDU_COLUMN_TYPE_NOT_SUPPORT, "暂不支持该类型" + schemaColumn);
            }

            KuduPredicate predicate = KuduPredicate.newComparisonPredicate(schemaColumn, comparisonOp, row);
            LOG.info("kudu table predicate schemaColumn={},comparisonOp={},row={}", schemaColumn, comparisonOp, row);

            kuduScannerBuilder.addPredicate(predicate);
        }
    }
}