package admin.service;

import admin.entity.TesseractTrigger;
import admin.pojo.TriggerVO;
import com.baomidou.mybatisplus.extension.service.IService;

import java.text.ParseException;
import java.util.List;

/**
 * <p>
 * 服务类
 * </p>
 *
 * @author nickle
 * @since 2019-07-03
 */
public interface ITesseractTriggerService extends IService<TesseractTrigger> {
    List<TesseractTrigger> findTriggerWithLock(String groupName, int batchSize, long time, Integer timeWindowSize);

    TriggerVO listByPage(Integer currentPage, Integer pageSize,
                         TesseractTrigger condition,
                         Long startCreateTime,
                         Long endCreateTime);

    void executeTrigger(String groupName, Integer triggerId);

    void startTrigger(Integer triggerId) throws ParseException;

    void stopTrigger(Integer triggerId);

    void deleteTrigger(Integer triggerId);

    void saveOrUpdateTrigger(TesseractTrigger tesseractTrigger) throws Exception;


    List<TesseractTrigger> listMissfire(Integer pageSize);
}
