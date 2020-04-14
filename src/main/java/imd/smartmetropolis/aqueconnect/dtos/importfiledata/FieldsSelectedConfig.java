package imd.smartmetropolis.aqueconnect.dtos.importfiledata;

import java.util.Map;

public class FieldsSelectedConfig {
    private Map<String, Object> importNGSILDDataConfig;
    private Map<String, Integer> fieldsSelected;

    public Map<String, Object> getImportNGSILDDataConfig() {
        return importNGSILDDataConfig;
    }

    public void setImportNGSILDDataConfig(Map<String, Object> importNGSILDDataConfig) {
        this.importNGSILDDataConfig = importNGSILDDataConfig;
    }

    public Map<String, Integer> getFieldsSelected() {
        return fieldsSelected;
    }

    public void setFieldsSelected(Map<String, Integer> fieldsSelected) {
        this.fieldsSelected = fieldsSelected;
    }


}
