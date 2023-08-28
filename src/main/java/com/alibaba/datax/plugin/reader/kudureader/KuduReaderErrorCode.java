package com.alibaba.datax.plugin.reader.kudureader;


import com.alibaba.datax.common.spi.ErrorCode;

public enum KuduReaderErrorCode implements ErrorCode {
    KUDU_PARAM_NECESSARY("KUDU_ErrCode-1", "参数必须存在"),
    KUDU_COLUMN_TYPE_NOT_SUPPORT("KUDU_ErrCode-2", "kudu列的类型暂不支持"),
    KUDU_ONLY_ONE_CONNECTION("KUDU_ErrCode-3", "kuduReader只能配置一个kuduConnection");
    private final String code;
    private final String describe;

    KuduReaderErrorCode(String code, String describe) {
        this.code = code;
        this.describe = describe;
    }


    @Override
    public String getCode() {
        return code;
    }

    @Override
    public String getDescription() {
        return describe;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Describe:[%s]. ", this.code,
                this.describe);
    }
}
