package admin.controller;


import admin.entity.TesseractTrigger;
import admin.pojo.VO.CommonResponseVO;
import admin.pojo.VO.TriggerVO;
import admin.service.ITesseractTriggerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.StringUtils;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import tesseract.exception.TesseractException;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.text.ParseException;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author nickle
 * @since 2019-07-03
 */
@RestController
@RequestMapping("/tesseract-trigger")
@Validated
public class TesseractTriggerController {
    @Autowired
    private ITesseractTriggerService triggerService;


    @GetMapping("/triggerList")
    public CommonResponseVO tesseractTriggerList(@NotNull @Min(1) Integer currentPage
            , @NotNull @Min(1) @Max(50) Integer pageSize, TesseractTrigger condition,
                                                 Long startCreateTime,
                                                 Long endCreateTime,
                                                 Long startUpdateTime,
                                                 Long endUpdateTime) {
        TriggerVO tesseractTriggerIPage = triggerService.listByPage(currentPage, pageSize
                , condition, startCreateTime, endCreateTime);
        return CommonResponseVO.success(tesseractTriggerIPage);
    }

    @PostMapping("/addTrigger")
    public CommonResponseVO addTrigger(@Validated @RequestBody TesseractTrigger tesseractTrigger) throws Exception {
        triggerService.saveOrUpdateTrigger(tesseractTrigger);
        return CommonResponseVO.SUCCESS;
    }

    @RequestMapping("/execute")
    public CommonResponseVO execute(String groupName, @NotNull Integer triggerId) {
        if (StringUtils.isEmpty(groupName)) {
            throw new TesseractException("请先给触发器所属执行器添加组");
        }
        triggerService.executeTrigger(groupName, triggerId);
        return CommonResponseVO.SUCCESS;
    }

    @RequestMapping("/start")
    public CommonResponseVO start(@NotNull Integer triggerId) throws ParseException {
        triggerService.startTrigger(triggerId);
        return CommonResponseVO.SUCCESS;
    }

    @RequestMapping("/stop")
    public CommonResponseVO stop(@NotNull Integer triggerId) {
        triggerService.stopTrigger(triggerId);
        return CommonResponseVO.SUCCESS;
    }

    @RequestMapping("/delete")
    public CommonResponseVO delete(@NotNull Integer triggerId) {
        triggerService.deleteTrigger(triggerId);
        return CommonResponseVO.SUCCESS;
    }
}

