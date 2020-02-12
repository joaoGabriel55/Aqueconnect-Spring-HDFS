package imd.smartmetropolis.aqueconnect.dtos.importfiledata;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ImportNGSILDDataWithoutContextConfig {
    private List<GeoLocationConfig> geoLocationConfig;
    private List<Map<String, Object>> dataContentForNGSILDConversion;

    public List<GeoLocationConfig> getGeoLocationConfig() {
        return geoLocationConfig;
    }

    public void setGeoLocationConfig(List<GeoLocationConfig> geoLocationConfig) {
        this.geoLocationConfig = geoLocationConfig;
    }

    public List<Map<String, Object>> getDataContentForNGSILDConversion() {
        if (dataContentForNGSILDConversion == null) {
            dataContentForNGSILDConversion = new ArrayList<>();
        }
        return dataContentForNGSILDConversion;
    }

    public void setDataContentForNGSILDConversion(List<Map<String, Object>> dataContentForNGSILDConversion) {
        this.dataContentForNGSILDConversion = dataContentForNGSILDConversion;
    }
}
