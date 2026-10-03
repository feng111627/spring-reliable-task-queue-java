package com.github.feng111627.springreliabletaskqueuejava.web;

import com.github.feng111627.springreliabletaskqueuejava.queue.CancelResult;
import com.github.feng111627.springreliabletaskqueuejava.queue.SubmitResult;
import com.github.feng111627.springreliabletaskqueuejava.queue.TaskQueueService;
import com.github.feng111627.springreliabletaskqueuejava.queue.TaskRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.List;

/**
 * 任务队列 REST 接口：提交（幂等，支持优先级/定时）、查询、取消、死信列表与再投递。
 */
@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskQueueService service;

    public TaskController(TaskQueueService service) {
        this.service = service;
    }

    /**
     * 提交请求。
     * priority：可选，整数越大越优先，缺省 0；
     * notBefore：可选，ISO-8601 时间（如 2026-10-03T12:00:00Z），到点前任务不可被消费。
     */
    public record SubmitRequest(String businessKey, String payload, Integer priority, Instant notBefore) {
    }

    @PostMapping
    public ResponseEntity<SubmitResult> submit(@RequestBody SubmitRequest request) {
        SubmitResult result = service.submit(
                request.businessKey(), request.payload(), request.priority(), request.notBefore());
        // 重复提交返回 200 但 outcome=DUPLICATE，调用方可明确区分
        return ResponseEntity.ok(result);
    }

    @GetMapping("/id/{taskId}")
    public ResponseEntity<TaskRecord> getById(@PathVariable String taskId) {
        return service.findById(taskId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * 取消任务。HTTP 200 时通过 body.outcome 区分：CANCELLED（本次取消）/ALREADY_CANCELLED（重复取消）
     * /ALREADY_SUCCEEDED/NOT_CANCELLABLE（执行中）；任务不存在返回 404。
     */
    @PostMapping("/{taskId}/cancel")
    public ResponseEntity<CancelResult> cancel(@PathVariable String taskId) {
        CancelResult result = service.cancel(taskId);
        if (result.outcome() == CancelResult.Outcome.NOT_FOUND) {
            return ResponseEntity.status(404).body(result);
        }
        return ResponseEntity.ok(result);
    }

    @GetMapping("/{businessKey}")
    public ResponseEntity<TaskRecord> getByBusinessKey(@PathVariable String businessKey) {
        return service.findByBusinessKey(businessKey)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public List<TaskRecord> listAll() {
        return service.listAll();
    }

    @GetMapping("/dead-letters")
    public List<TaskRecord> deadLetters() {
        return service.listDeadLetters();
    }

    @PostMapping("/dead-letters/{taskId}/redeliver")
    public ResponseEntity<Boolean> redeliver(@PathVariable String taskId) {
        boolean ok = service.redeliverDeadLetter(taskId);
        return ok ? ResponseEntity.ok(true) : ResponseEntity.notFound().build();
    }
}
