package imd.smartmetropolis.aqueconnect.services;

import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

@Component
public interface FileJsonConverterService {
    List<Map<String, Object>> jsonConverter(String contentFile, Map<String, Integer> fieldsSelected);
}
