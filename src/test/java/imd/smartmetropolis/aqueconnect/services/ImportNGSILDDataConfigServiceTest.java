package imd.smartmetropolis.aqueconnect.services;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.FieldsSelectedConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.ImportNGSILDDataConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.context.ImportNGSILDDataWithContextConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.context.MatchingConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.standard.GeoLocationConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.standard.ImportNGSILDDataWithoutContextConfig;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ImportNGSILDDataConfigServiceTest {

    private final ImportNGSILDDataConfigService service = ImportNGSILDDataConfigService.getServiceInstance();

    private List<GeoLocationConfig> getGeolocationConfig() {
        List<GeoLocationConfig> geoLocationConfig = new ArrayList<>();
        GeoLocationConfig config = new GeoLocationConfig();
        config.setKey("test");
        config.setTypeGeolocation("Point");
        config.setTypeOfSelection("geojson");
        geoLocationConfig.add(config);
        return geoLocationConfig;
    }

    private List<MatchingConfig> getMatchingConfig() {
        List<MatchingConfig> matchingConfigContent = new ArrayList<>();
        MatchingConfig config = new MatchingConfig();
        config.setContextName("test_context");
        config.setForeignProperty("test_foreign");
        config.setGeoLocationConfig(getGeolocationConfig());
        config.setLocation(true);
        matchingConfigContent.add(config);
        return matchingConfigContent;
    }

    @Test
    public void getInstanceImportConfigContextTest() {
        ImportNGSILDDataConfig config = service.getInstanceImportConfig("context");
        assertTrue((config instanceof ImportNGSILDDataWithContextConfig));
    }

    @Test
    public void getInstanceImportConfigStandard() {
        ImportNGSILDDataConfig config = service.getInstanceImportConfig("standard");
        assertTrue((config instanceof ImportNGSILDDataWithoutContextConfig));
    }

    @Test
    public void mountImportConfigStandardTest() {
        ImportNGSILDDataConfig config = service.getInstanceImportConfig("standard");
        FieldsSelectedConfig fieldsSelectedConfig = new FieldsSelectedConfig();
        fieldsSelectedConfig.setImportNGSILDDataWithoutContextConfig(new ImportNGSILDDataWithoutContextConfig());
        ImportNGSILDDataWithoutContextConfig standardConfig = fieldsSelectedConfig.getImportNGSILDDataWithoutContextConfig();
        standardConfig.setGeoLocationConfig(getGeolocationConfig());
        standardConfig.setPrimaryField("ID");
        standardConfig.setDataContentForNGSILDConversion(new ArrayList<>());

        service.mountImportConfigStandard(fieldsSelectedConfig, (ImportNGSILDDataWithoutContextConfig) config);

        assertEquals(
                standardConfig.getGeoLocationConfig(),
                ((ImportNGSILDDataWithoutContextConfig) config).getGeoLocationConfig()
        );
    }

    @Test
    public void mountImportConfigContextTest() {
        ImportNGSILDDataConfig config = service.getInstanceImportConfig("context");
        FieldsSelectedConfig fieldsSelectedConfig = new FieldsSelectedConfig();
        fieldsSelectedConfig.setImportNGSILDDataWithContextConfig(new ImportNGSILDDataWithContextConfig());
        ImportNGSILDDataWithContextConfig standardConfig = fieldsSelectedConfig.getImportNGSILDDataWithContextConfig();
        standardConfig.setContextLinks(Arrays.asList("context1", "context2"));
        standardConfig.setMatchingConfigContent(getMatchingConfig());
        standardConfig.setPrimaryField("id");
        standardConfig.setDataContentForNGSILDConversion(new ArrayList<>());

        service.mountImportConfigContext(fieldsSelectedConfig, (ImportNGSILDDataWithContextConfig) config);

        assertEquals(standardConfig.getContextLinks(), ((ImportNGSILDDataWithContextConfig) config).getContextLinks());
        assertEquals(
                standardConfig.getMatchingConfigContent(),
                ((ImportNGSILDDataWithContextConfig) config).getMatchingConfigContent()
        );
    }
}
