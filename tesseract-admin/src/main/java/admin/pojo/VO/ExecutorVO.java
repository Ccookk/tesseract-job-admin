package admin.pojo.VO;

import lombok.Data;

import java.util.List;

@Data
public class ExecutorVO {
    private PageVO pageInfo;
    private List<TesseractExecutorVO> executorList;
}
