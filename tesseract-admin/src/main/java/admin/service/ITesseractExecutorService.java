package admin.service;

import admin.entity.TesseractExecutor;
import admin.pojo.VO.ExecutorVO;
import com.baomidou.mybatisplus.extension.service.IService;
import tesseract.core.dto.TesseractAdminRegistryRequest;
import tesseract.core.dto.TesseractAdminRegistryResDTO;

import java.util.List;

/**
 * <p>
 * 服务类
 * </p>
 *
 * @author nickle
 * @since 2019-07-03
 */
public interface ITesseractExecutorService extends IService<TesseractExecutor> {
    TesseractAdminRegistryResDTO registry(TesseractAdminRegistryRequest tesseractAdminRegistryRequest) throws Exception;

    ExecutorVO listByPage(Long currentPage, Long pageSize,
                          TesseractExecutor condition,
                          Long startCreateTime,
                          Long endCreateTime);

    void saveOrUpdateExecutor(TesseractExecutor tesseractExecutor);

    void deleteExecutor(Integer executorId);

    List<TesseractExecutor> executorListNoDetail(Integer groupId);
}
