package imd.smartmetropolis.aqueconnect.dtos.importfiledata;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class GeoLocationConfig {

    private String key;
    private String typeOfSelection;
    private boolean invertCoords;
    private String delimiter;
    private String typeGeolocation;

}