package imd.smartmetropolis.aqueconnect.dtos.importfiledata;

import java.util.Map;

//TODO: Add importNGSILDDataWithContextConfig
public class FieldsSelectedConfig {
    private ImportNGSILDDataWithoutContextConfig importNGSILDDataWithoutContextConfig;
    private Map<String, Integer> fieldsSelected;

    public ImportNGSILDDataWithoutContextConfig getImportNGSILDDataWithoutContextConfig() {
        return importNGSILDDataWithoutContextConfig;
    }

    public void setImportNGSILDDataWithoutContextConfig(ImportNGSILDDataWithoutContextConfig importNGSILDDataWithoutContextConfig) {
        this.importNGSILDDataWithoutContextConfig = importNGSILDDataWithoutContextConfig;
    }

    public Map<String, Integer> getFieldsSelected() {
        return fieldsSelected;
    }

    public void setFieldsSelected(Map<String, Integer> fieldsSelected) {
        this.fieldsSelected = fieldsSelected;
    }


}
