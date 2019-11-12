package tesseract.core.dto;

import lombok.Data;

import java.io.Serializable;

@Data
public class TesseractExecutorRequest implements Serializable {
    private static final long serialVersionUID = 1L;

    private String className;

    private Integer shardingIndex;


    private Long logId;


    private Integer triggerId;


    private Integer jobId;


    private Integer executorDetailId;


    private Integer fireJobId;

    private String params;
}
