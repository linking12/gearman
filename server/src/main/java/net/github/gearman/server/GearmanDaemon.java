package net.github.gearman.server;

import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.yaml.snakeyaml.Yaml;

import net.github.gearman.server.config.DefaultServerConfiguration;
import net.github.gearman.server.config.GearmanServerConfiguration;
import net.github.gearman.server.net.ServerListener;

@SpringBootApplication
public class GearmanDaemon implements CommandLineRunner {

    private static final Logger LOG = LoggerFactory.getLogger(GearmanDaemon.class);

    public static void main(String[] args) {
        SpringApplication.run(GearmanDaemon.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        final String configFile;
        if (args.length != 1) {
            configFile = "config.yml";
        } else {
            configFile = args[0];
        }

        final GearmanServerConfiguration serverConfiguration = loadFromConfigOrGenerateDefaultConfig(configFile);
        final ServerListener serverListener = new ServerListener(serverConfiguration);

        try {
            serverListener.start();
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }

    private GearmanServerConfiguration loadFromConfigOrGenerateDefaultConfig(final String configFile) {
        GearmanServerConfiguration serverConfiguration = null;
        try (InputStream in = GearmanDaemon.class.getClassLoader().getResourceAsStream(configFile)) {
            Yaml yaml = new Yaml();
            serverConfiguration = yaml.loadAs(in, GearmanServerConfiguration.class);
            System.out.println(serverConfiguration.toString());
            LOG.info("Starting Gearman Server with settings from " + configFile + "...");
        } catch (Exception e) {
            LOG.error("Can't load " + configFile + ": ", e);
        }

        if (serverConfiguration == null) {
            LOG.info("Starting Gearman Server with default settings ...");
            serverConfiguration = new DefaultServerConfiguration();
        }

        return serverConfiguration;
    }

}
