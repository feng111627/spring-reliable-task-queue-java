package com.github.feng111627.springreliabletaskqueuejava.taskqueue;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** REST 接口：提交任务（幂等）、查询状态、死信列表与再投递 */
@RestController
@RequestMapping("/api/tasks")
public class TaskQueueController {

    private final TaskQueueService service;

    public TaskQueueController(TaskQueueService service) {
        this.service = service;
    }

    public record SubmitRequest(String businessKey, String payload) {
    }

    public record SubmitResponse(String status, String taskId, String businessKey, String taskStatus) {
    }

    /** 提交任务；重复提交返回 200 + DUPLICATE，新任务返回 201 + ACCEPTED */
    @PostMapping
    public ResponseEntity<SubmitResponse> submit(@RequestBody SubmitRequest request) {
        SubmitResult result = service.submit(request.businessKey(), request.payload());
        Task task = result.getTask();
        SubmitResponse body = new SubmitResponse(
                result.getStatus().name(), task.getId(), task.getBusinessKey(), task.getStatus().name());
        return ResponseEntity
                .status(result.isAccepted() ? HttpStatus.CREATED : HttpStatus.OK)
                .body(body);
    }

    @GetMapping("/{id}")
    public Task get(@PathVariable String id) {
        return service.getTask(id);
    }

    @GetMapping
    public List<Task> listAll() {
        return service.listAll();
    }

    @GetMapping("/dead-letters")
    public List<Task> listDeadLetters() {
        return service.listDeadLetters();
    }

    @PostMapping("/dead-letters/{id}/redeliver")
    public Task redeliver(@PathVariable String id) {
        return service.redeliver(id);
    }

    @org.springframework.web.bind.annotation.ExceptionHandler({IllegalArgumentException.class, IllegalStateException.class})
    public ResponseEntity<Map<String, String>> handleBadRequest(RuntimeException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
