package imd.smartmetropolis.aqueconnect.dtos.importfiledata;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

//TODO: Add importNGSILDDataWithContextConfig
@Getter
@Setter
@NoArgsConstructor
public class FieldsSelectedConfig {
    private ImportNGSILDDataWithoutContextConfig importNGSILDDataWithoutContextConfig;
    private Map<String, Integer> fieldsSelected;
}
