package com.github.feng111627.springreliabletaskqueuejava.taskqueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/** 任务队列装配：存储 -> 服务 -> 调度器，启动时恢复、停机时收尾 */
@Configuration
@EnableConfigurationProperties(TaskQueueProperties.class)
public class TaskQueueConfig {

    private static final Logger log = LoggerFactory.getLogger(TaskQueueConfig.class);

    @Bean
    public TaskStore taskStore(TaskQueueProperties properties) {
        FileTaskStore store = new FileTaskStore(properties);
        // 启动恢复：加载本地文件，把中断的 RUNNING 任务重新置为 PENDING
        store.recover();
        log.info("[启动恢复] 任务存储已加载, 文件={}, 当前任务数={}", properties.getStoragePath(), store.listAll().size());
        return store;
    }

    @Bean
    public TaskQueueService taskQueueService(TaskStore taskStore, TaskQueueProperties properties) {
        return new TaskQueueService(taskStore, properties);
    }

    /** 默认处理器：仅打印日志；业务方可声明自己的 TaskHandler Bean 覆盖 */
    @Bean
    @ConditionalOnMissingBean
    public TaskHandler taskHandler() {
        return task -> log.info("[默认处理器] 执行任务 id={} businessKey={} payload={}",
                task.getId(), task.getBusinessKey(), task.getPayload());
    }

    @Bean(destroyMethod = "stop")
    public TaskDispatcher taskDispatcher(TaskStore taskStore, TaskHandler taskHandler, TaskQueueProperties properties) {
        TaskDispatcher dispatcher = new TaskDispatcher(taskStore, taskHandler, properties);
        dispatcher.start();
        return dispatcher;
    }
}
