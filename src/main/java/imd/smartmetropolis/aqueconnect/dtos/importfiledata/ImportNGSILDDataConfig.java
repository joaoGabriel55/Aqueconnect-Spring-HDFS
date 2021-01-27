package imd.smartmetropolis.aqueconnect.dtos.importfiledata;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
public class ImportNGSILDDataConfig {

    private String primaryField;
    private List<String> contextLinks;
    private LinkedHashMap<String, MatchingConverterSetup> matchingConverterSetup;
    private List<Map<String, Object>> dataCollection;

    public ImportNGSILDDataConfig() {
    }

    public ImportNGSILDDataConfig(String primaryField, List<String> contextLinks, LinkedHashMap<String, MatchingConverterSetup> matchingConverterSetup, List<Map<String, Object>> dataCollection) {
        this.primaryField = primaryField;
        this.contextLinks = contextLinks;
        this.matchingConverterSetup = matchingConverterSetup;
        this.dataCollection = dataCollection;
    }

    public List<Map<String, Object>> getDataCollection() {
        if (this.dataCollection == null)
            return this.dataCollection = new ArrayList<>();
        return this.dataCollection;
    }

    public void setDataCollection(List<Map<String, Object>> dataCollection) {
        this.dataCollection = dataCollection;
    }

}
