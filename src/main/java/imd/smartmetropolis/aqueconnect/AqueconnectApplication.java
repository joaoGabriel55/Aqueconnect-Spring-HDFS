package imd.smartmetropolis.aqueconnect;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.servlet.support.SpringBootServletInitializer;

@SpringBootApplication
public class AqueconnectApplication extends SpringBootServletInitializer {

    public static void main(String[] args) {
        SpringApplication.run(AqueconnectApplication.class, args);
    }

    @Override
    protected SpringApplicationBuilder configure(SpringApplicationBuilder application) {
        return application.sources(applicationClass);
    }

    private static Class<AqueconnectApplication> applicationClass = AqueconnectApplication.class;

}
