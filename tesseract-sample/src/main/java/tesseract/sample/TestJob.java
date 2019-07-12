package tesseract.sample;

import org.springframework.stereotype.Component;
import tesseract.core.annotation.TesseractJob;
import tesseract.core.context.ExecutorContext;
import tesseract.core.handler.JobHandler;

@TesseractJob(triggerName = "testTrigger")
@Component
public class TestJob implements JobHandler {
    @Override
    public void execute(ExecutorContext executorContext) throws Exception {
        System.out.println("任务执行开始");
        Thread.sleep(30 * 1000);
        System.out.println("任务执行结束");
    }
}
