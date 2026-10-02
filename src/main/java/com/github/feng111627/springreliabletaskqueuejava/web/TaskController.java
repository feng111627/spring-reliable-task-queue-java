package com.github.feng111627.springreliabletaskqueuejava.web;

import com.github.feng111627.springreliabletaskqueuejava.queue.SubmitResult;
import com.github.feng111627.springreliabletaskqueuejava.queue.TaskQueueService;
import com.github.feng111627.springreliabletaskqueuejava.queue.TaskRecord;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 任务队列 REST 接口：提交（幂等）、查询、死信列表与再投递。
 */
@RestController
@RequestMapping("/api/tasks")
public class TaskController {

    private final TaskQueueService service;

    public TaskController(TaskQueueService service) {
        this.service = service;
    }

    public record SubmitRequest(String businessKey, String payload) {
    }

    @PostMapping
    public ResponseEntity<SubmitResult> submit(@RequestBody SubmitRequest request) {
        SubmitResult result = service.submit(request.businessKey(), request.payload());
        // 重复提交返回 200 但 outcome=DUPLICATE，调用方可明确区分
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
