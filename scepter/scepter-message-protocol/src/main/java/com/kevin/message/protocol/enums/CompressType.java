package com.kevin.message.protocol.enums;


import com.kevin.message.protocol.compress.ICompress;
import com.kevin.message.protocol.compress.UnCompress;

/**
 * @author: kevin
 * @description: 压缩算法枚举
 * @updateRemark: 更新项目
 * @date: 2019-07-29 17:31
 */
public enum CompressType {

    /**
     * 不压缩(无意义编号为0)
     */
    UnCompress(0, new UnCompress()),

    /**
     * ZIP压缩
     */
    ZIP(1, null),

    /**
     * 7zip
     */
    SevenZip(2, null);

    private final int code;

    private final ICompress compress;

    private CompressType(int code, ICompress compress) {
        this.code = code;
        this.compress = compress;
    }

    public int getCode() {
        return code;
    }

    public ICompress getCompress() {
        return compress;
    }

    /**
     * 根据压缩编码获取枚举
     *
     * @param code - int
     * @return CompressType
     * @throws Exception
     */
    public static CompressType getCompressType(int code) throws Exception {
        for (CompressType type : CompressType.values()) {
            if (type.code == code) {
                return type;
            }
        }
        throw new Exception("末知的压缩格式");
    }

}
