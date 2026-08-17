package ru.otus.hw;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequiredArgsConstructor
@Slf4j
public class EventCountController {
    private final EventCountService service;

    /** GET http://localhost:8080/count?key=user1 */
    @GetMapping("/count")
    public Map<String, Object> getCount(@RequestParam String key) {
        try {
            return Map.of("key", key, "sessions", service.count(key));
        } catch (Exception e) {
            log.error("Exception", e);
            return Map.of("key", key, "error", String.valueOf(e.getMessage()));
        }
    }
}
