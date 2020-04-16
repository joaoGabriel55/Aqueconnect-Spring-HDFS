package imd.smartmetropolis.aqueconnect.dtos.importfiledata.context;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.ImportNGSILDDataConfig;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;

@Getter
@Setter
@NoArgsConstructor
public class ImportNGSILDDataWithContextConfig extends ImportNGSILDDataConfig {
    private List<String> contextLinks;
    private List<MatchingConfig> matchingConfigContent;
}
