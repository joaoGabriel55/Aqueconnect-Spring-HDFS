package imd.smartmetropolis.aqueconnect.services;

import imd.smartmetropolis.aqueconnect.dtos.importfiledata.FieldsSelectedConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.ImportNGSILDDataConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.context.ImportNGSILDDataWithContextConfig;
import imd.smartmetropolis.aqueconnect.dtos.importfiledata.standard.ImportNGSILDDataWithoutContextConfig;

public class ImportNGSILDDataConfigService {

    private static ImportNGSILDDataConfigService instance;

    public static ImportNGSILDDataConfigService getServiceInstance() {
        if (instance == null)
            return new ImportNGSILDDataConfigService();
        return instance;
    }

    public ImportNGSILDDataConfig getInstanceImportConfig(String typeImportSetup) {
        if (typeImportSetup.equals("context"))
            return new ImportNGSILDDataWithContextConfig();
        else if (typeImportSetup.equals("standard"))
            return new ImportNGSILDDataWithoutContextConfig();
        return null;
    }

    public void mountImportConfigStandard(
            FieldsSelectedConfig fieldsSelectedConfig, ImportNGSILDDataWithoutContextConfig importConfig
    ) {
        importConfig.setGeoLocationConfig(
                fieldsSelectedConfig.getImportNGSILDDataWithoutContextConfig().getGeoLocationConfig()
        );
        if (fieldsSelectedConfig.getImportNGSILDDataWithoutContextConfig().getPrimaryField() != null) {
            String primaryField = fieldsSelectedConfig
                    .getImportNGSILDDataWithoutContextConfig()
                    .getPrimaryField().trim().toLowerCase();
            importConfig.setPrimaryField(primaryField);
        }
    }

    public void mountImportConfigContext(
            FieldsSelectedConfig fieldsSelectedConfig, ImportNGSILDDataWithContextConfig importConfig
    ) {
        importConfig.setMatchingConfigContent(
                fieldsSelectedConfig.getImportNGSILDDataWithContextConfig().getMatchingConfigContent()
        );
        importConfig.setContextLinks(fieldsSelectedConfig.getImportNGSILDDataWithContextConfig().getContextLinks());
        if (fieldsSelectedConfig.getImportNGSILDDataWithContextConfig().getPrimaryField() != null) {
            String primaryField = fieldsSelectedConfig
                    .getImportNGSILDDataWithContextConfig()
                    .getPrimaryField().trim().toLowerCase();
            importConfig.setPrimaryField(primaryField);
        }
    }
}
