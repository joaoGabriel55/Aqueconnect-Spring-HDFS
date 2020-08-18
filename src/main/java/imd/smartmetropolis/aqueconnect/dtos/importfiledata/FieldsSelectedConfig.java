package imd.smartmetropolis.aqueconnect.dtos.importfiledata;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
public class FieldsSelectedConfig {
    private Map<String, Integer> fieldsSelected;
    private ImportNGSILDDataConfig importNGSILDDataConfig;
}
