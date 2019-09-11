package admin.controller;


import admin.service.ITesseractExecutorDetailService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author nickle
 * @since 2019-07-07
 */
@RestController
@RequestMapping("/tesseract-executor-detail")
@Validated
public class TesseractExecutorDetailController {
    @Autowired
    private ITesseractExecutorDetailService executorDetailService;

}
