package imd.smartmetropolis.aqueconnect.dtos.importfiledata.standard;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.ImportNGSILDDataConfig;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
public class ImportNGSILDDataWithoutContextConfig extends ImportNGSILDDataConfig {
    private List<GeoLocationConfig> geoLocationConfig;
}
