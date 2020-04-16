package imd.smartmetropolis.aqueconnect.dtos.importfiledata;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.context.ImportNGSILDDataWithContextConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.standard.ImportNGSILDDataWithoutContextConfig;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Map;

@Getter
@Setter
@NoArgsConstructor
public class FieldsSelectedConfig {
    private ImportNGSILDDataWithoutContextConfig importNGSILDDataWithoutContextConfig;
    private ImportNGSILDDataWithContextConfig importNGSILDDataWithContextConfig;
    private Map<String, Integer> fieldsSelected;
}
