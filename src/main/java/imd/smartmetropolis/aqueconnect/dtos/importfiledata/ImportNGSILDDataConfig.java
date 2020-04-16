package imd.smartmetropolis.aqueconnect.dtos.importfiledata;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
abstract public class ImportNGSILDDataConfig {
    private String primaryField;
    private List<Map<String, Object>> dataContentForNGSILDConversion;

    public List<Map<String, Object>> getDataContentForNGSILDConversion() {
        if (this.dataContentForNGSILDConversion == null)
            return this.dataContentForNGSILDConversion = new ArrayList<>();
        return this.dataContentForNGSILDConversion;
    }

    public void setDataContentForNGSILDConversion(List<Map<String, Object>> dataContentForNGSILDConversion) {
        this.dataContentForNGSILDConversion = dataContentForNGSILDConversion;
    }
}
