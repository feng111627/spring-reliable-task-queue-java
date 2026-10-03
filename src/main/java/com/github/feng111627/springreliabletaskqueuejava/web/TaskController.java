package com.github.feng111627.springreliabletaskqueuejava.web;

import com.github.feng111627.springreliabletaskqueuejava.queue.CancelResult;
import com.github.feng111627.springreliabletaskqueuejava.queue.SubmitResult;
import com.github.feng111627.springreliabletaskqueuejava.queue.TaskQueueService;
import com.github.feng111627.springreliabletaskqueuejava.queue.TaskRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.List;

/**
 * 任务队列 REST 接口：提交（幂等，支持优先级与定时投递）、查询、取消、死信列表与再投递。
 */
@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskQueueService service;

    public TaskController(TaskQueueService service) {
        this.service = service;
    }

    /**
     * 提交请求：
     * priority  可选，整数越大越优先，缺省 0（与旧行为一致）
     * notBefore 可选，ISO-8601 时间（如 2026-10-03T12:00:00Z），到点前不可消费
     */
    public record SubmitRequest(String businessKey, String payload, Integer priority, String notBefore) {
    }

    @PostMapping
    public ResponseEntity<SubmitResult> submit(@RequestBody SubmitRequest request) {
        Instant notBefore = null;
        if (request.notBefore() != null && !request.notBefore().isBlank()) {
            try {
                notBefore = Instant.parse(request.notBefore());
            } catch (DateTimeParseException e) {
                throw new IllegalArgumentException("notBefore 必须是 ISO-8601 时间，例如 2026-10-03T12:00:00Z");
            }
        }
        int priority = request.priority() == null ? 0 : request.priority();
        SubmitResult result = service.submit(request.businessKey(), request.payload(), priority, notBefore);
        // 重复提交返回 200 但 outcome=DUPLICATE，调用方可明确区分
        return ResponseEntity.ok(result);
    }

    @GetMapping("/{businessKey}")
    public ResponseEntity<TaskRecord> getByBusinessKey(@PathVariable String businessKey) {
        return service.findByBusinessKey(businessKey)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping("/id/{taskId}")
    public ResponseEntity<TaskRecord> getById(@PathVariable String taskId) {
        return service.findById(taskId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @PostMapping("/{taskId}/cancel")
    public ResponseEntity<CancelResult> cancel(@PathVariable String taskId,
                                               @RequestBody(required = false) CancelRequest request) {
        String reason = request == null ? null : request.reason();
        CancelResult result = service.cancel(taskId, reason);
        return switch (result.outcome()) {
            case CANCELLED, ALREADY_CANCELLED, RUNNING, ALREADY_FINISHED -> ResponseEntity.ok(result);
            case NOT_FOUND -> ResponseEntity.status(404).body(result);
        };
    }

    public record CancelRequest(String reason) {
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
